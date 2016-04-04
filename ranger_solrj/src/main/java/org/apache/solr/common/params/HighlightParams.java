/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.solr.common.params;

/**
 *
 * @since solr 1.3
 */
public interface HighlightParams {
  String HIGHLIGHT   = "hl";
  String Q           = HIGHLIGHT+".q";
  String QPARSER     = HIGHLIGHT+".qparser";
  String FIELDS      = HIGHLIGHT+".fl";
  String SNIPPETS    = HIGHLIGHT+".snippets";
  String FRAGSIZE    = HIGHLIGHT+".fragsize";
  String INCREMENT   = HIGHLIGHT+".increment";
  String MAX_CHARS   = HIGHLIGHT+".maxAnalyzedChars";
  String FORMATTER   = HIGHLIGHT+".formatter";
  String ENCODER     = HIGHLIGHT+".encoder";
  String FRAGMENTER  = HIGHLIGHT+".fragmenter";
  String PRESERVE_MULTI = HIGHLIGHT+".preserveMulti";
  String FRAG_LIST_BUILDER = HIGHLIGHT+".fragListBuilder";
  String FRAGMENTS_BUILDER = HIGHLIGHT+".fragmentsBuilder";
  String BOUNDARY_SCANNER = HIGHLIGHT+".boundaryScanner";
  String BS_MAX_SCAN = HIGHLIGHT+".bs.maxScan";
  String BS_CHARS = HIGHLIGHT+".bs.chars";
  String BS_TYPE = HIGHLIGHT+".bs.type";
  String BS_LANGUAGE = HIGHLIGHT+".bs.language";
  String BS_COUNTRY = HIGHLIGHT+".bs.country";
  String BS_VARIANT = HIGHLIGHT+".bs.variant";
  String FIELD_MATCH = HIGHLIGHT+".requireFieldMatch";
  String DEFAULT_SUMMARY = HIGHLIGHT + ".defaultSummary";
  String ALTERNATE_FIELD = HIGHLIGHT+".alternateField";
  String ALTERNATE_FIELD_LENGTH = HIGHLIGHT+".maxAlternateFieldLength";
  String MAX_MULTIVALUED_TO_EXAMINE = HIGHLIGHT + ".maxMultiValuedToExamine";
  String MAX_MULTIVALUED_TO_MATCH = HIGHLIGHT + ".maxMultiValuedToMatch";
  
  String USE_PHRASE_HIGHLIGHTER = HIGHLIGHT+".usePhraseHighlighter";
  String HIGHLIGHT_MULTI_TERM = HIGHLIGHT+".highlightMultiTerm";
  String PAYLOADS = HIGHLIGHT+".payloads";

  String MERGE_CONTIGUOUS_FRAGMENTS = HIGHLIGHT + ".mergeContiguous";

  String USE_FVH  = HIGHLIGHT + ".useFastVectorHighlighter";
  String TAG_PRE  = HIGHLIGHT + ".tag.pre";
  String TAG_POST = HIGHLIGHT + ".tag.post";
  String TAG_ELLIPSIS = HIGHLIGHT + ".tag.ellipsis";
  String PHRASE_LIMIT = HIGHLIGHT + ".phraseLimit";
  String MULTI_VALUED_SEPARATOR = HIGHLIGHT + ".multiValuedSeparatorChar";
  
  // Formatter
  String SIMPLE = "simple";
  String SIMPLE_PRE  = HIGHLIGHT+"."+SIMPLE+".pre";
  String SIMPLE_POST = HIGHLIGHT+"."+SIMPLE+".post";

  // Regex fragmenter
  String REGEX = "regex";
  String SLOP  = HIGHLIGHT+"."+REGEX+".slop";
  String PATTERN  = HIGHLIGHT+"."+REGEX+".pattern";
  String MAX_RE_CHARS   = HIGHLIGHT+"."+REGEX+".maxAnalyzedChars";
  
  // Scoring parameters
  String SCORE = "score";
  String SCORE_K1 = HIGHLIGHT +"."+SCORE+".k1";
  String SCORE_B = HIGHLIGHT +"."+SCORE+".b";
  String SCORE_PIVOT = HIGHLIGHT +"."+SCORE+".pivot";
}
