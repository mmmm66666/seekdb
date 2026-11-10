/*
 * Copyright (c) 2025 OceanBase.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#define USING_LOG_PREFIX STORAGE_FTS

#include "storage/fts/ik/ob_ik_arbitrator.h"

#include "lib/allocator/ob_allocator.h"
#include "lib/allocator/page_arena.h"
#include "lib/ob_errno.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/utility.h"
#include "share/rc/ob_server_runtime.h"
#include "storage/fts/ik/ob_ik_processor.h"
#include "storage/fts/ik/ob_ik_token.h"

namespace oceanbase
{
namespace storage
{
int ObIKArbitrator::process(TokenizeContext &ctx)
{
  int ret = OB_SUCCESS;

  ObList<ObIKToken, ObIAllocator> &tokens = ctx.token_list().tokens();
  ObIKTokenChain *chain_need_arbitrate = nullptr;
  bool use_smart = ctx.is_smart();
  if (OB_ISNULL(chain_need_arbitrate = OB_NEWx(ObIKTokenChain, &alloc_, alloc_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc memory failed");
  } else {
    while (OB_SUCC(ret) && !tokens.empty()) {
      ObIKToken token = tokens.get_first();
      bool is_add = false;
      if (OB_FAIL(tokens.pop_front())) {
      } else if (OB_FAIL(chain_need_arbitrate->add_token_if_conflict(token, is_add))) {
      } else if (!is_add) {
        ObFTSortList::CellIter iter = chain_need_arbitrate->list().tokens().begin();
        ObIKTokenChain *judge_result = nullptr;
        if (chain_need_arbitrate->list().tokens().size() == 1 || !use_smart) {
          if (OB_FAIL(add_chain(chain_need_arbitrate))) {
          } else {
            // add ok
          }
        } else if (OB_FAIL(optimize(ctx,
                                    chain_need_arbitrate,
                                    iter,
                                    chain_need_arbitrate->offset_len(),
                                    judge_result))) {
        } else if (OB_FAIL(add_chain(judge_result))) {
        } else {
          // add best chain and delete origin chain
          OB_DELETEx(ObIKTokenChain, &alloc_, chain_need_arbitrate);
        }

        // start a new chain
        if (OB_FAIL(ret)) {
        } else if (OB_ISNULL(chain_need_arbitrate = OB_NEWx(ObIKTokenChain, &alloc_, alloc_))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("alloc memory failed");
        } else if (OB_FAIL(chain_need_arbitrate->add_token_if_conflict(token, is_add))) {
        } else {
          // Add first, it should be added.
        }
      } else {
        // still use same cross chain
      }
    }

    // handle last chain
    ObIKTokenChain *judge_result = nullptr;
    if (OB_FAIL(ret)) {
    } else if (chain_need_arbitrate->list().tokens().size() == 1 || !use_smart) {
      if (OB_FAIL(add_chain(chain_need_arbitrate))) {
      }
    } else if (OB_FAIL(optimize(ctx,
                                chain_need_arbitrate,
                                chain_need_arbitrate->list().tokens().begin(),
                                chain_need_arbitrate->offset_len(),
                                judge_result))) {
    } else if (OB_FAIL(add_chain(judge_result))) {
      OB_DELETEx(ObIKTokenChain, &alloc_, judge_result);
    } else {
    }
  }

  // if failed, free chain_need_arbitrate
  if (OB_FAIL(ret)) {
    OB_DELETEx(ObIKTokenChain, &alloc_, chain_need_arbitrate);
  }

  return ret;
}

namespace
{
// Fetch the byte length of the character starting at `offset` and, when
// `update_type` is set, its character type as well. The segmentation pass already
// decoded and classified every character, so prefer its recorded result and only
// decode again for positions it did not visit. The produced values are identical
// to a direct decode, this only avoids repeating the work.
int get_char_info(TokenizeContext &ctx,
                  const int64_t offset,
                  int64_t &char_len,
                  ObFTCharUtil::CharType &type,
                  const bool update_type)
{
  int ret = OB_SUCCESS;
  uint8_t cached_len = 0;
  ObFTCharUtil::CharType cached_type = ObFTCharUtil::CharType::USELESS;

  if (OB_SUCC(ctx.get_cached_char(offset, cached_type, cached_len))) {
    char_len = static_cast<int64_t>(cached_len);
    if (update_type) {
      type = cached_type;
    }
  } else if (OB_ENTRY_NOT_EXIST == ret) {
    ret = OB_SUCCESS;
    ObFTCharUtil::CharType decoded_type = ObFTCharUtil::CharType::USELESS;
    if (OB_FAIL(ObCharset::first_valid_char(ctx.collation(),
                                            ctx.fulltext() + offset,
                                            ctx.fulltext_len() - offset,
                                            char_len))) {
    } else if (!update_type) {
      // keep the caller's type, it is intentionally reused across the gap
    } else if (OB_FAIL(ObFTCharUtil::classify_first_char(ctx.collation(),
                                                         ctx.fulltext() + offset,
                                                         static_cast<uint8_t>(char_len),
                                                         decoded_type))) {
    } else {
      type = decoded_type;
    }
  }
  return ret;
}
} // namespace

int ObIKArbitrator::output_result(TokenizeContext &ctx)
{
  int ret = OB_SUCCESS;

  int64_t char_len = 0;
  int32_t chain_idx = 0;
  ObIKTokenChain *chain = nullptr;
  for (int64_t current = 0; OB_SUCC(ret) && current < ctx.fulltext_len();) {
    ObFTCharUtil::CharType type = ObFTCharUtil::CharType::USELESS;
    // maybe not so good to keep single, check it later
    if (OB_FAIL(get_char_info(ctx, current, char_len, type, true))) {
    } else if (ObFTCharUtil::CharType::USELESS == type) {
      current += char_len; // skip useless char
    } else if (OB_FAIL(get_chain_at(current, chain_idx, chain)) && OB_HASH_NOT_EXIST != ret) {
    } else if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      if (!keep_single()) {
        // skip single word
      } else {
        // output single word
        ObIKToken token;
        token.offset_ = current;
        token.length_ = char_len;
        token.ptr_ = ctx.fulltext();
        token.char_cnt_ = 1;
        bool is_ignore = false;
        if (ObFTCharUtil::CharType::CHINESE == type) {
          token.type_ = ObIKTokenType::IK_CHINESE_TOKEN;
          if (OB_FAIL(ctx.result_list().push_back(token))) {
          }
        } else if (ObFTCharUtil::CharType::OTHER_CJK == type) {
          if (OB_FAIL(ObFTCharUtil::is_ignore_single_cjk(ctx.collation(),
                                                         ctx.fulltext() + current,
                                                         char_len,
                                                         is_ignore))) {
          } else if (!is_ignore && !FALSE_IT(token.type_ = ObIKTokenType::IK_OTHER_CJK_TOKEN)
                     && OB_FAIL(ctx.result_list().push_back(token))) {
          } else {
            // ignore
          }
        }
      }
      current += char_len;
    } else {
      for (ObList<ObIKToken, ObIAllocator>::const_iterator iter = chain->list().tokens().begin();
           OB_SUCC(ret) && iter != chain->list().tokens().end();
           iter++) {
        const ObIKToken &token = *iter;
        if (!keep_single()) {
          current = token.offset_;
        } else {
          // output single word between two token
          while (OB_SUCC(ret) && current < token.offset_) {
            if (OB_FAIL(get_char_info(ctx, current, char_len, type, false))) {
              break;
            } else {
              ObIKToken token;
              token.offset_ = current;
              token.length_ = char_len;
              token.ptr_ = ctx.fulltext();
              token.char_cnt_ = 1;
              bool is_ignore = false;
              if (ObFTCharUtil::CharType::CHINESE == type) {
                token.type_ = ObIKTokenType::IK_CHINESE_TOKEN;
                ctx.result_list().push_back(token);
              } else if (ObFTCharUtil::CharType::OTHER_CJK == type) {
                if (OB_FAIL(ObFTCharUtil::is_ignore_single_cjk(ctx.collation(),
                                                               ctx.fulltext() + current,
                                                               char_len,
                                                               is_ignore))) {
                } else if (!is_ignore) {
                  token.type_ = ObIKTokenType::IK_OTHER_CJK_TOKEN;
                  ctx.result_list().push_back(token);
                } else {
                }
              }
              current += char_len;
            }
          }
        }

        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(ctx.result_list().push_back(token))) {
        } else
          // output the token
          current = token.offset_ + token.length_;
      }
    }
    if (OB_FAIL(ret)) {
      break;
    } else {
      // pass
    }
  }

  return ret;
}

int ObIKArbitrator::optimize(TokenizeContext &ctx,
                             ObIKTokenChain *chain,
                             ObFTSortList::CellIter iter,
                             int64_t fulltext_len,
                             ObIKTokenChain *&best)
{
  int ret = OB_SUCCESS;

  ObIKTokenChain *option = nullptr;
  ObList<ObFTSortList::CellIter, ObIAllocator> conflict_stack(alloc_);

  if (OB_ISNULL(option = OB_NEWx(ObIKTokenChain, &alloc_, alloc_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc memory failed");
  } else if (OB_FAIL(try_add_next_words(chain, iter, option, true, conflict_stack))) {
  } else if (OB_ISNULL(best = OB_NEWx(ObIKTokenChain, &alloc_, alloc_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Alloc memory failed");
  } else if (OB_FAIL(best->copy(option))) {
  } else {
    while (OB_SUCC(ret) && !conflict_stack.empty()) {
      ObFTSortList::CellIter iter = conflict_stack.get_last();
      conflict_stack.pop_back();
      if (OB_FAIL(remove_conflict(*iter, option))) {
      } else if (OB_FAIL(try_add_next_words(chain, iter, option, false, conflict_stack))) {
      } else if (option->better_than(*best)) {
        OB_DELETEx(ObIKTokenChain, &alloc_, best);
        if (OB_ISNULL(best = OB_NEWx(ObIKTokenChain, &alloc_, alloc_))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("Alloc memory failed");
        } else if (OB_FAIL(best->copy(option))) {
        } else {
          // best option is copied
        }
      } else {
        // ignore worse chain
      }
    }
  }

  // any way, free option
  OB_DELETEx(ObIKTokenChain, &alloc_, option);

  if (OB_FAIL(ret)) {
    OB_DELETEx(ObIKTokenChain, &alloc_, best);
  }
  return ret;
}

int ObIKArbitrator::try_add_next_words(ObIKTokenChain *chain,
                                       ObFTSortList::CellIter iter,
                                       ObIKTokenChain *option,
                                       bool need_conflict,
                                       ObList<ObFTSortList::CellIter, ObIAllocator> &conflict_stack)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(chain) || OB_ISNULL(option)) {
    ret = OB_INVALID_ARGUMENT;
  }

  while (OB_SUCC(ret) && iter != chain->list().tokens().end()) {
    bool is_add = false;
    if (OB_FAIL(option->add_token_if_no_conflict(*iter, is_add))) {
    } else if (!is_add && need_conflict && OB_FAIL(conflict_stack.push_back(iter))) {
    } else {
      // no add or push back over
      iter++;
    }
  }

  return ret;
}

int ObIKArbitrator::remove_conflict(const ObIKToken &token, ObIKTokenChain *option)
{
  int ret = OB_SUCCESS;
  ObIKToken tmp;
  while (OB_SUCC(ret) && option->check_conflict(token)) {
    if (OB_FAIL(option->pop_back(tmp))) {
    }
  }
  return ret;
}

int ObIKArbitrator::get_chain_at(const int64_t offset,
                                 int32_t &chain_idx,
                                 ObIKTokenChain *&chain) const
{
  int ret = OB_HASH_NOT_EXIST;
  int32_t idx = chain_idx;
  // chains are appended in ascending min_offset order, so a cursor is enough to
  // find the chain starting at the current text position
  while (idx < chains_.size() && chains_[idx]->min_offset() < offset) {
    ++idx;
  }
  chain_idx = idx;
  if (idx < chains_.size() && chains_[idx]->min_offset() == offset) {
    chain = chains_[idx];
    ret = OB_SUCCESS;
  }
  return ret;
}

ObIKArbitrator::ObIKArbitrator()
    : alloc_(lib::ObMemAttr("IK Arbitrator")), chains_(&alloc_)
{
}

int ObIKArbitrator::add_chain(ObIKTokenChain *chain)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(chain)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (chain->list().is_empty()) {
    // no need to add empty chain
  } else if (OB_UNLIKELY(0 < chains_.size()
                         && chains_[chains_.size() - 1]->min_offset() >= chain->min_offset())) {
    // output_result() relies on the chains being ordered by start position; the
    // hash map this replaces rejected a duplicate start position as well.
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected chain order", K(ret), K(chain->min_offset()),
             K(chains_[chains_.size() - 1]->min_offset()));
  } else if (OB_FAIL(chains_.push_back(chain))) {
  }
  return ret;
}

ObIKArbitrator::~ObIKArbitrator()
{
  // alloc_ owns every chain and its tokens, so releasing it frees them all.
  // Members are destroyed in reverse declaration order: chains_ first, then the
  // arena it allocated from.
}
} // namespace storage
} // namespace oceanbase
