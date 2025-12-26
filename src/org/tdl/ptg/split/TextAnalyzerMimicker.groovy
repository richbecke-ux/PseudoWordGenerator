package org.tdl.ptg.split

import groovy.transform.Canonical
import groovy.json.JsonSlurper
import groovy.json.JsonOutput
import java.nio.file.Files
import java.nio.file.Paths

// ================================================================
// PSEUDO TEXT GENERATOR - Fixed-Depth Markov Edition
// ================================================================
//
// Key Design:
// 1. Fixed-depth Markov (default 3) for sentence structure
// 2. Sentence length controlled by observed distribution
// 3. Nested segments (QUOTE, PAREN, etc.) have their own chains
// 4. Attribution is a post-QUOTE specialization, not in chain
//
// Entity types in chain:
//   WORD:SHORT, WORD:MED, WORD:LONG  (binned word lengths)
//   PAUSE:,  PAUSE:;  PAUSE::        (clause pauses)
//   SEG:QUOTE, SEG:PAREN, etc.       (nested segment markers)
//   END                               (sentence/segment end)
//
// ================================================================

// ----------------------------------------------------------------
// --- GLOBALS ---
// ----------------------------------------------------------------

class Globals {
    static final Random RND = new Random()
    static final Set<String> VOWELS = ['a','e','i','o','u','y','æ','ø','å','ä','ö','ü','é'] as Set
    static final Set<String> TERMINATORS = ['.', '!', '?', '\u2026'] as Set
    static final Set<String> PAUSES = [',', ';', ':'] as Set
    static final int MAX_EFF_LEN = 8

    // Markov context depth for sentence structure
    static int STRUCT_CONTEXT_DEPTH = 3

    // Special markers
    static final String ATTR_MARKER = '\uE000'
    static final String ATTR_END = '\uE001'
}

// ----------------------------------------------------------------
// --- DATA CLASSES ---
// ----------------------------------------------------------------

@Canonical
class Token {
    String type = 'WORD'     // WORD, TERM, PAUSE, OPEN, CLOSE, LINK, ATTR_START, ATTR_END
    String text = ''         // Original text (for words)
    String value = ''        // Punctuation character
    String context = ''      // QUOTE, PAREN, SENTENCE, etc.
    int wordLength = 0       // CV-structure length
}

@Canonical
class Segment {
    String type = 'SENTENCE'
    List<Integer> wordLengths = []
    List<Segment> children = []
    int insertPosition = -1
    String terminator = ''
    boolean startsWithQuote = false
    Attribution attribution = null
}

@Canonical
class Attribution {
    List<Integer> nameLengths = []
    List<Integer> titleLengths = []
    boolean hasTitle = false
}

// ----------------------------------------------------------------
// --- WEIGHTED SELECTOR ---
// ----------------------------------------------------------------

@Canonical
class WeightedSelector {
    List<String> keys = []
    List<Long> cumulative = []
    long total = 0

    WeightedSelector(Map<String, Integer> freq) {
        long running = 0L
        freq.sort { -it.value }.each { k, v ->
            if (v > 0) {
                keys << k
                running += v
                cumulative << running
            }
        }
        total = running
    }

    String select() {
        if (total <= 0 || keys.isEmpty()) return null
        long target = (long)(Globals.RND.nextDouble() * total) + 1
        int idx = Collections.binarySearch(cumulative, target)
        if (idx < 0) idx = -idx - 1
        return keys[Math.min(idx, keys.size() - 1)]
    }

    boolean hasKey(String k) { keys.contains(k) }
}

// ----------------------------------------------------------------
// --- UTILITIES ---
// ----------------------------------------------------------------

class Utils {
    static List tokenizeCV(String word) {
        def clean = word.findAll { Character.isLetter(it as char) }.join('').toLowerCase()
        if (!clean) return []
        def tokens = []
        def curType = null
        def curContent = new StringBuilder()

        clean.each { c ->
            def type = Globals.VOWELS.contains(c) ? 'V' : 'C'
            if (curType == type) {
                curContent.append(c)
            } else {
                if (curType != null) tokens << [curType, curContent.toString()]
                curType = type
                curContent = new StringBuilder(c)
            }
        }
        if (curType != null) tokens << [curType, curContent.toString()]
        return tokens
    }

    static String binWordLength(int len) {
        if (len <= 3) return "SHORT"
        if (len <= 6) return "MED"
        return "LONG"
    }

    static int unbinWordLength(String bin) {
        switch (bin) {
            case "SHORT": return Globals.RND.nextInt(3) + 1    // 1-3
            case "MED":   return Globals.RND.nextInt(4) + 4    // 4-7
            case "LONG":  return Globals.RND.nextInt(4) + 8    // 8-11
            default:      return Globals.RND.nextInt(4) + 4    // default MED
        }
    }

    static String smartCaps(String s) {
        if (!s) return s
        def m = (s =~ /[a-zA-Z]/)
        if (m.find()) {
            int i = m.start()
            return s.substring(0, i) + s[i].toUpperCase() + s.substring(i + 1)
        }
        return s
    }

    static String titleCase(String s) {
        if (!s || s.length() == 0) return s
        return s[0].toUpperCase() + (s.length() > 1 ? s.substring(1) : '')
    }
}

// ----------------------------------------------------------------
// --- PUNCTUATION ---
// ----------------------------------------------------------------

class Punctuation {
    static boolean isApostrophe(String text, int pos) {
        if (pos <= 0 || pos >= text.length() - 1) return false
        return text[pos-1] ==~ /\w/ && text[pos+1] ==~ /\w/
    }

    static Map<String,String> classify(String c) {
        switch (c) {
            case '.': case '!': case '?':
                return [type: 'TERM', value: c, context: 'SENTENCE']
            case '\u2026':
                return [type: 'TERM', value: c, context: 'ELLIPSIS']
            case ',': case ';':
                return [type: 'PAUSE', value: c, context: 'CLAUSE']
            case ':':
                return [type: 'PAUSE', value: c, context: 'INTRO']
            case '\u2014':
                return [type: 'LINK', value: c, context: 'DASH']
            case '-':
                return [type: 'LINK', value: c, context: 'HYPHEN']
            case '(': return [type: 'OPEN', value: c, context: 'PAREN']
            case ')': return [type: 'CLOSE', value: c, context: 'PAREN']
            case '[': return [type: 'OPEN', value: c, context: 'BRACKET']
            case ']': return [type: 'CLOSE', value: c, context: 'BRACKET']
            case '\u201C': case '\u201D': case '"':
                return [type: 'QUOTE', value: '"', context: 'QUOTE']
            case '\u2018': case '\u2019': case '\'':
                return [type: 'SQUOTE', value: "'", context: 'SQUOTE']
            case Globals.ATTR_MARKER:
                return [type: 'ATTR_START', value: '\u2014', context: 'ATTR']
            case Globals.ATTR_END:
                return [type: 'ATTR_END', value: '', context: 'ATTR']
            default:
                return null
        }
    }

    static String guessQuoteDir(String prev, String next) {
        def prevSpace = !prev || prev ==~ /.*[\s(\[{—]$/
        def nextWord = next && next ==~ /^\w.*/
        return (prevSpace && nextWord) ? 'OPEN' : 'CLOSE'
    }
}

// ----------------------------------------------------------------
// --- TEXT PREPROCESSOR ---
// ----------------------------------------------------------------

class Preprocessor {
    String process(String text) {
        if (!text) return ''

        // BOM
        if (text.charAt(0) == '\uFEFF') text = text.substring(1)

        // Normalize dashes
        text = text.replaceAll(/--+/, '\u2014')

        // Normalize quotes
        text = normalizeQuotes(text)

        // Mark attributions BEFORE other processing
        // Attribution = closing quote + em-dash + Name + newline
        // The attribution serves as sentence terminator
        text = markAttributions(text)

        // Remove invalid dashes: space-dash-nonspace (not attribution)
        text = text.replaceAll(/(?<!\S)\u2014(?=\S)(?!\uE000)/, '')

        // Normalize valid dashes (space on both sides)
        text = text.replaceAll(/\s*\u2014\s*/, ' \u2014 ')

        // Process line by line to handle newlines correctly
        def lines = text.split(/\r?\n/)
        def result = new StringBuilder()

        for (int i = 0; i < lines.length; i++) {
            def line = lines[i].trim()
            if (!line) {
                // Empty line (paragraph break)
                // Only add period if previous content doesn't end with terminator
                if (result.length() > 0) {
                    def lastChar = result.charAt(result.length() - 1)

                    // Check if we need to add a terminator
                    if (lastChar != '.' && lastChar != '!' && lastChar != '?' &&
                            lastChar != (Globals.ATTR_END as char)) {
                        // Need to add period
                        // If last char is closing quote, insert period before it
                        if (lastChar == '\u201D' || lastChar == ('"' as char)) {
                            result.deleteCharAt(result.length() - 1)
                            result.append('.').append(lastChar)
                        } else {
                            result.append('.')
                        }
                    }
                }
                continue
            }

            // Add space before this line if there's previous content
            if (result.length() > 0) {
                result.append(' ')
            }
            result.append(line)
        }

        return result.toString().replaceAll(/\s+/, ' ').trim()
    }

    private String normalizeQuotes(String text) {
        def sb = new StringBuilder()
        def chars = text.toCharArray()

        for (int i = 0; i < chars.length; i++) {
            def c = chars[i]
            if (c == '"' as char) {
                def prev = i > 0 ? chars[i-1].toString() : ''
                def next = i < chars.length-1 ? chars[i+1].toString() : ''
                sb.append(Punctuation.guessQuoteDir(prev, next) == 'OPEN' ? '\u201C' : '\u201D')
            } else if (c == '\'' as char) {
                if (Punctuation.isApostrophe(text, i)) {
                    sb.append('\u2019')
                } else {
                    def prev = i > 0 ? chars[i-1].toString() : ''
                    def next = i < chars.length-1 ? chars[i+1].toString() : ''
                    sb.append(Punctuation.guessQuoteDir(prev, next) == 'OPEN' ? '\u2018' : '\u2019')
                }
            } else {
                sb.append(c)
            }
        }
        return sb.toString()
    }

    private String markAttributions(String text) {
        // Attribution patterns:
        // 1. "Quote text." —Name[, title]  (quoted attribution)
        // 2. Sentence text. —Name[, title]  (unquoted attribution like sayings/teachings)
        // Name may start with * for italics (like *Song of All*)

        // Pattern 1: Quote + em-dash + Name at end of line
        // [*]? allows optional asterisk for italic titles
        def quotePattern = ~/([""\u201C\u201D])\s*\u2014\s*([*]?[A-Z][^\r\n]*?)(\r?\n|$)/
        text = text.replaceAll(quotePattern) { match, quote, attrText, lineEnd ->
            "${quote} ${Globals.ATTR_MARKER}${attrText.trim()}${Globals.ATTR_END} "
        }

        // Pattern 2: Terminator + em-dash + Name at end of line (for unquoted attributions)
        def unquotedPattern = ~/([.!?])\s*\u2014\s*([*]?[A-Z][^\r\n\uE000]*?)(\r?\n|$)/
        text = text.replaceAll(unquotedPattern) { match, term, attrText, lineEnd ->
            "${term} ${Globals.ATTR_MARKER}${attrText.trim()}${Globals.ATTR_END} "
        }

        return text
    }
}

// ----------------------------------------------------------------
// --- TOKENIZER ---
// ----------------------------------------------------------------

class Tokenizer {
    List<Token> tokenize(String text) {
        def tokens = []
        text.split(/\s+/).each { word ->
            if (word) tokens.addAll(tokenizeWord(word))
        }
        return tokens.findAll { it != null }
    }

    private List<Token> tokenizeWord(String word) {
        def tokens = []
        def current = new StringBuilder()
        def chars = word.toCharArray()

        for (int i = 0; i < chars.length; i++) {
            def c = chars[i].toString()

            // Special markers always get tokenized
            if (c == Globals.ATTR_MARKER || c == Globals.ATTR_END) {
                if (current) {
                    def wt = makeWordToken(current.toString())
                    if (wt) tokens << wt
                    current = new StringBuilder()
                }
                def p = Punctuation.classify(c)
                if (p) tokens << new Token(type: p.type, value: p.value, context: p.context)
                continue
            }

            // Check if this is an apostrophe (single quote between word chars)
            // Apostrophe = U+2019 or straight quote with word char on both sides
            def straightQuote = "'"
            if ((c == '\u2019' || c == straightQuote || c == '\u2018') &&
                    i > 0 && i < chars.length - 1) {
                def prev = chars[i-1].toString()
                def next = chars[i+1].toString()
                if (prev.matches('[a-zA-Z0-9]') && next.matches('[a-zA-Z0-9]')) {
                    // It is an apostrophe - keep it as part of the word
                    current.append(c)
                    continue
                }
            }

            def p = Punctuation.classify(c)
            if (p) {
                if (current) {
                    def wt = makeWordToken(current.toString())
                    if (wt) tokens << wt
                    current = new StringBuilder()
                }
                tokens << new Token(type: p.type, value: p.value, context: p.context)
            } else {
                current.append(c)
            }
        }

        if (current) {
            def wt = makeWordToken(current.toString())
            if (wt) tokens << wt
        }

        return tokens
    }

    private Token makeWordToken(String text) {
        def cv = Utils.tokenizeCV(text)
        if (!cv) return null
        def hasVowel = text.toLowerCase().any { Globals.VOWELS.contains(it) }
        if (!hasVowel) return null
        return new Token(type: 'WORD', text: text, wordLength: cv.size())
    }
}

// ----------------------------------------------------------------
// --- STRUCTURE PARSER ---
// ----------------------------------------------------------------

class StructureParser {
    // Quote state tracking
    private Map<String, Integer> quoteStack = [QUOTE: 0, SQUOTE: 0]

    List<Segment> parse(List<Token> tokens) {
        def segments = []
        def current = new Segment(type: 'SENTENCE')
        def stack = [current]
        quoteStack = [QUOTE: 0, SQUOTE: 0]

        boolean inAttr = false
        Attribution attr = null
        boolean inTitle = false

        tokens.eachWithIndex { tok, idx ->
            switch (tok.type) {
                case 'WORD':
                    if (inAttr) {
                        if (inTitle) attr.titleLengths << tok.wordLength
                        else attr.nameLengths << tok.wordLength
                    } else {
                        stack[0].wordLengths << tok.wordLength
                    }
                    break

                case 'QUOTE':
                case 'SQUOTE':
                    handleQuote(tok.type, stack, tokens, idx, current)
                    break

                case 'OPEN':
                    def seg = new Segment(type: tok.context, insertPosition: stack[0].wordLengths.size())
                    stack[0].children << seg
                    stack.add(0, seg)
                    break

                case 'CLOSE':
                    if (stack.size() > 1 && stack[0].type == tok.context) {
                        stack.remove(0)
                    }
                    break

                case 'LINK':
                    if (tok.context == 'DASH') {
                        handleDash(stack, tokens, idx)
                    }
                    break

                case 'ATTR_START':
                    // Attribution is valid if we're at sentence level (stack.size=1)
                    if (stack.size() == 1) {
                        inAttr = true
                        attr = new Attribution()
                        inTitle = false
                    }
                    break

                case 'ATTR_END':
                    // Attribution end always marks sentence boundary
                    if (inAttr && attr) {
                        current.attribution = attr
                    }
                    // Add segment if it has content OR has attribution
                    if (current.wordLengths || current.children || current.attribution) {
                        segments << current
                    }
                    current = new Segment(type: 'SENTENCE')
                    stack = [current]
                    quoteStack = [QUOTE: 0, SQUOTE: 0]
                    inAttr = false
                    attr = null
                    break

                case 'PAUSE':
                    if (inAttr && tok.value == ',') {
                        inTitle = true
                        attr.hasTitle = true
                    } else if (!inAttr && stack[0].wordLengths) {
                        def pause = new Segment(type: 'PAUSE', terminator: tok.value,
                                insertPosition: stack[0].wordLengths.size())
                        stack[0].children << pause
                    }
                    break

                case 'TERM':
                    if (!inAttr) {
                        // Check if we're inside a quote (anywhere on stack)
                        def quoteOnStack = stack.find { it.type in ['QUOTE', 'SQUOTE'] }

                        if (quoteOnStack) {
                            // Terminator is inside a quote - store it with the quote
                            // Don't end sentence - wait for quote to close
                            quoteOnStack.terminator = tok.value
                        } else {
                            // Terminator at sentence level (possibly inside DASH_ASIDE)
                            // This ends the sentence (and any open DASH_ASIDE)
                            current.terminator = tok.value

                            // Close any open segments (like DASH_ASIDE)
                            while (stack.size() > 1) {
                                stack.remove(0)
                            }
                            quoteStack = [QUOTE: 0, SQUOTE: 0]

                            if (current.wordLengths || current.children) {
                                segments << current
                            }
                            current = new Segment(type: 'SENTENCE')
                            stack = [current]
                        }
                    }
                    break
            }
        }

        // Handle unterminated sentence
        if (current.wordLengths || current.children) {
            segments << current
        }

        return segments
    }

    private void handleQuote(String qtype, List<Segment> stack, List<Token> tokens, int idx, Segment current) {
        // SQUOTE is only valid inside a QUOTE segment
        // If we see SQUOTE and we're not inside a QUOTE, ignore it
        if (qtype == 'SQUOTE') {
            boolean insideQuote = stack.any { it.type == 'QUOTE' }
            if (!insideQuote) {
                return  // Ignore single quotes outside double quotes
            }
        }

        // Determine open/close from context
        def isOpen = shouldOpen(qtype, tokens, idx)

        if (isOpen) {
            quoteStack[qtype]++
            def seg = new Segment(type: qtype, insertPosition: stack[0].wordLengths.size())

            // Check if quote starts sentence (only for QUOTE, not SQUOTE)
            if (qtype == 'QUOTE' && stack.size() == 1 && stack[0].wordLengths.isEmpty() && stack[0].children.isEmpty()) {
                current.startsWithQuote = true
            }

            stack[0].children << seg
            stack.add(0, seg)
        } else {
            // Closing a quote - pop any nested segments (like DASH_ASIDE) first
            if (quoteStack[qtype] > 0) {
                // Find the quote on the stack
                def quoteIdx = stack.findIndexOf { it.type == qtype }
                if (quoteIdx >= 0) {
                    // Pop everything up to and including the quote
                    while (stack.size() > 1 && stack[0].type != qtype) {
                        stack.remove(0)
                    }
                    if (stack.size() > 1 && stack[0].type == qtype) {
                        quoteStack[qtype]--
                        stack.remove(0)
                    }
                }
            }
        }
    }

    private boolean shouldOpen(String qtype, List<Token> tokens, int idx) {
        // Simple logic: if we have an unmatched open quote of this type, this should close it
        if (quoteStack[qtype] > 0) return false
        return true
    }

    private void handleDash(List<Segment> stack, List<Token> tokens, int idx) {
        // Don't open a dash-aside if the next token is a quote (end of quoted speech with dash)
        if (idx + 1 < tokens.size() && tokens[idx + 1].type in ['QUOTE', 'SQUOTE']) {
            return  // Skip - this is likely trailing dash in "I—" pattern
        }

        // Dash-aside toggle: if one is open, close it; otherwise open new
        def hasOpen = stack.any { it.type == 'DASH_ASIDE' }
        if (hasOpen) {
            while (stack.size() > 1 && stack[0].type != 'DASH_ASIDE') {
                stack.remove(0)
            }
            if (stack.size() > 1) stack.remove(0)
        } else {
            def aside = new Segment(type: 'DASH_ASIDE', insertPosition: stack[0].wordLengths.size())
            stack[0].children << aside
            stack.add(0, aside)
        }
    }
}

// ----------------------------------------------------------------
// --- MARKOV CHAIN ANALYZER ---
// ----------------------------------------------------------------

class MarkovAnalyzer {
    // Per-segment-type chains: type -> (context -> (next -> count))
    Map<String, Map<String, Map<String, Integer>>> chains = [:]

    // Length distributions per segment type
    Map<String, List<Integer>> lengthDists = [:]

    // Terminator distribution
    Map<String, Integer> terminators = [:]

    // Attribution stats
    int quoteSentences = 0          // Sentences starting with quote
    int quoteSentencesWithAttr = 0  // Those that have attribution
    List<Integer> attrNameWordCounts = []   // How many words in each name
    List<Integer> attrNameLengths = []      // Individual word lengths
    List<Integer> attrTitleWordCounts = []  // How many words in each title
    List<Integer> attrTitleLengths = []     // Individual word lengths
    double attrTitleProbability = 0.0

    void analyze(List<Segment> segments) {
        segments.each { seg ->
            analyzeSegment(seg)

            // Track attribution stats
            if (seg.startsWithQuote) {
                quoteSentences++
            }

            // Count attributions (both quoted and unquoted)
            if (seg.attribution) {
                if (seg.startsWithQuote) {
                    quoteSentencesWithAttr++
                }
                // Store word count AND individual lengths
                attrNameWordCounts << seg.attribution.nameLengths.size()
                attrNameLengths.addAll(seg.attribution.nameLengths)
                if (seg.attribution.hasTitle) {
                    attrTitleWordCounts << seg.attribution.titleLengths.size()
                    attrTitleLengths.addAll(seg.attribution.titleLengths)
                }
            }

            // Track terminators
            if (seg.terminator && !seg.attribution) {
                terminators.merge(seg.terminator, 1, { a, b -> a + b })
            }
        }

        // Calculate attribution probability
        if (quoteSentences > 0) {
            attrTitleProbability = quoteSentencesWithAttr > 0 ?
                    (attrTitleWordCounts.size() / (double)quoteSentencesWithAttr) : 0.0
        }
    }

    private void analyzeSegment(Segment seg) {
        def type = seg.type

        // Build entity stream
        def stream = buildEntityStream(seg)

        if (stream.isEmpty()) return

        // Track length
        lengthDists.computeIfAbsent(type, {[]}) << stream.size()

        // Build n-gram chain
        def depth = Globals.STRUCT_CONTEXT_DEPTH

        for (int i = 0; i < stream.size(); i++) {
            def ctx = buildContext(stream, i, depth)
            def next = stream[i]

            chains.computeIfAbsent(type, {[:]})
                    .computeIfAbsent(ctx, {[:]})
                    .merge(next, 1, { a, b -> a + b })
        }

        // Add END transition
        def endCtx = buildContext(stream, stream.size(), depth)
        chains.computeIfAbsent(type, {[:]})
                .computeIfAbsent(endCtx, {[:]})
                .merge('END', 1, { a, b -> a + b })

        // Recurse into children
        seg.children.each { child ->
            if (child.type != 'PAUSE') {
                analyzeSegment(child)
            }
        }
    }

    private List<String> buildEntityStream(Segment seg) {
        def stream = []
        def childMap = [:] // position -> child segment

        seg.children.each { child ->
            if (child.type == 'PAUSE') {
                childMap[child.insertPosition] = "PAUSE:${child.terminator}"
            } else {
                childMap[child.insertPosition] = "SEG:${child.type}"
            }
        }

        seg.wordLengths.eachWithIndex { len, i ->
            // Insert any children at this position first
            if (childMap.containsKey(i)) {
                stream << childMap[i]
            }
            stream << "WORD:${Utils.binWordLength(len)}"
        }

        // Any children at end position
        if (childMap.containsKey(seg.wordLengths.size())) {
            stream << childMap[seg.wordLengths.size()]
        }

        return stream
    }

    private String buildContext(List<String> stream, int pos, int depth) {
        def ctx = []
        for (int i = depth; i >= 1; i--) {
            int idx = pos - i
            ctx << (idx >= 0 ? stream[idx] : 'START')
        }
        return ctx.join('|')
    }

    Map toJson() {
        return [
                chains: chains,
                lengthDists: lengthDists,
                terminators: terminators,
                quoteSentences: quoteSentences,
                quoteSentencesWithAttr: quoteSentencesWithAttr,
                attrNameWordCounts: attrNameWordCounts,
                attrNameLengths: attrNameLengths,
                attrTitleWordCounts: attrTitleWordCounts,
                attrTitleLengths: attrTitleLengths,
                contextDepth: Globals.STRUCT_CONTEXT_DEPTH
        ]
    }

    void fromJson(Map json) {
        chains = json.chains ?: [:]
        lengthDists = json.lengthDists ?: [:]
        terminators = json.terminators ?: [:]
        quoteSentences = json.quoteSentences ?: 0
        quoteSentencesWithAttr = json.quoteSentencesWithAttr ?: 0
        attrNameWordCounts = json.attrNameWordCounts ?: []
        attrNameLengths = json.attrNameLengths ?: []
        attrTitleWordCounts = json.attrTitleWordCounts ?: []
        attrTitleLengths = json.attrTitleLengths ?: []
        if (json.contextDepth) Globals.STRUCT_CONTEXT_DEPTH = json.contextDepth
    }
}

// ----------------------------------------------------------------
// --- WORD MODEL ANALYZER ---
// ----------------------------------------------------------------

class WordAnalyzer {
    Map<String, Integer> lengthStartStats = [:]   // "len:startType" -> count

    // Unigram: effLen -> prevType -> content -> count
    Map<String, Map<String, Map<String, Integer>>> startTokens = [:]
    Map<String, Map<String, Map<String, Integer>>> innerTokens = [:]
    Map<String, Map<String, Map<String, Integer>>> lastTokens = [:]

    // Bigram: effLen -> prevContent -> content -> count
    Map<String, Map<String, Map<String, Integer>>> bigramStart = [:]
    Map<String, Map<String, Map<String, Integer>>> bigramInner = [:]
    Map<String, Map<String, Map<String, Integer>>> bigramLast = [:]

    // Trigram: effLen -> "prev2:prev1" -> content -> count
    Map<String, Map<String, Map<String, Integer>>> trigramInner = [:]
    Map<String, Map<String, Map<String, Integer>>> trigramLast = [:]

    Set<String> vocabulary = new HashSet<>()

    void analyze(List<Token> tokens, boolean trackVocab) {
        tokens.findAll { it.type == 'WORD' && it.wordLength > 0 }.each { tok ->
            def clean = tok.text.findAll { Character.isLetter(it as char) }.join('').toLowerCase()
            def cv = Utils.tokenizeCV(tok.text)
            if (!cv) return

            if (trackVocab) vocabulary << clean

            def len = cv.size()
            def effLen = Math.min(len, Globals.MAX_EFF_LEN).toString()
            def startType = cv[0][0]

            lengthStartStats.merge("${len}:${startType}", 1, { a, b -> a + b })

            cv.eachWithIndex { t, i ->
                def content = t[1]
                def type = t[0]

                if (i == 0) {
                    ensure(startTokens, effLen, startType).merge(content, 1, { a, b -> a + b })
                } else {
                    def prev = cv[i-1]
                    def prevContent = prev[1]
                    def prevType = prev[0]
                    def isLast = (i == len - 1)

                    // Unigram
                    ensure(isLast ? lastTokens : innerTokens, effLen, prevType).merge(content, 1, { a, b -> a + b })

                    // Bigram
                    def bgMap = isLast ? bigramLast : (i == 1 ? bigramStart : bigramInner)
                    ensure(bgMap, effLen, prevContent).merge(content, 1, { a, b -> a + b })

                    // Trigram
                    if (i >= 2) {
                        def prev2 = cv[i-2][1]
                        def tgKey = "${prev2}:${prevContent}"
                        ensure(isLast ? trigramLast : trigramInner, effLen, tgKey).merge(content, 1, { a, b -> a + b })
                    }
                }
            }
        }
    }

    private Map<String, Integer> ensure(Map m, String k1, String k2) {
        m.computeIfAbsent(k1, {[:]}).computeIfAbsent(k2, {[:]})
    }

    Map toJson(boolean includeVocab) {
        return [
                vocabulary: includeVocab ? vocabulary : null,
                lengthStartStats: lengthStartStats,
                startTokens: startTokens,
                innerTokens: innerTokens,
                lastTokens: lastTokens,
                bigramStart: bigramStart,
                bigramInner: bigramInner,
                bigramLast: bigramLast,
                trigramInner: trigramInner,
                trigramLast: trigramLast
        ]
    }
}

// ----------------------------------------------------------------
// --- GENERATOR ---
// ----------------------------------------------------------------

class Generator {
    // Word model
    WeightedSelector lengthSelector
    Map<String, WeightedSelector> lengthStartSel = [:]
    Map<String, Map<String, WeightedSelector>> startSel = [:]
    Map<String, Map<String, WeightedSelector>> innerSel = [:]
    Map<String, Map<String, WeightedSelector>> lastSel = [:]
    Map<String, Map<String, WeightedSelector>> bgStartSel = [:]
    Map<String, Map<String, WeightedSelector>> bgInnerSel = [:]
    Map<String, Map<String, WeightedSelector>> bgLastSel = [:]
    Map<String, Map<String, WeightedSelector>> tgInnerSel = [:]
    Map<String, Map<String, WeightedSelector>> tgLastSel = [:]
    Set<String> vocabulary = null

    // Structure model
    Map<String, Map<String, WeightedSelector>> structChains = [:]
    Map<String, List<Integer>> lengthDists = [:]
    WeightedSelector termSelector
    int quoteSentences = 0
    int quoteSentencesWithAttr = 0
    List<Integer> attrNameWordCounts = []
    List<Integer> attrNameLengths = []
    List<Integer> attrTitleWordCounts = []
    List<Integer> attrTitleLengths = []

    int ngramMode = 3
    boolean markMode = false

    // Track the lowest n-gram level used during word generation
    int lastWordNgramLevel = 3  // 3=trigram, 2=bigram, 1=unigram

    void loadWordModel(String path, int pruneMin, boolean loadVocab = true) {
        def json = new JsonSlurper().parse(new File(path))

        if (loadVocab && json.vocabulary) vocabulary = new HashSet<>(json.vocabulary)

        Map<String, Integer> rawLen = json.lengthStartStats
        if (pruneMin > 0) {
            rawLen = rawLen.findAll { k, v -> k.split(':')[0].toInteger() > pruneMin }
        }
        lengthSelector = new WeightedSelector(rawLen)

        // Build length->startType selectors
        def tempMap = [:]
        json.lengthStartStats.each { k, v ->
            def parts = k.split(':')
            tempMap.computeIfAbsent(parts[0], {[:]}).put(parts[1], v)
        }
        tempMap.each { k, v -> lengthStartSel[k] = new WeightedSelector(v) }

        def buildSel = { src ->
            def dest = [:]
            src?.each { k1, inner ->
                dest[k1] = [:]
                inner.each { k2, map -> dest[k1][k2] = new WeightedSelector(map) }
            }
            return dest
        }

        startSel = buildSel(json.startTokens)
        innerSel = buildSel(json.innerTokens)
        lastSel = buildSel(json.lastTokens)
        bgStartSel = buildSel(json.bigramStart)
        bgInnerSel = buildSel(json.bigramInner)
        bgLastSel = buildSel(json.bigramLast)
        tgInnerSel = buildSel(json.trigramInner)
        tgLastSel = buildSel(json.trigramLast)
    }

    void loadStructModel(String path) {
        def json = new JsonSlurper().parse(new File(path))

        if (json.contextDepth) Globals.STRUCT_CONTEXT_DEPTH = json.contextDepth

        json.chains?.each { type, contexts ->
            structChains[type] = [:]
            contexts.each { ctx, nexts ->
                structChains[type][ctx] = new WeightedSelector(nexts)
            }
        }

        lengthDists = json.lengthDists ?: [:]

        if (json.terminators) {
            termSelector = new WeightedSelector(json.terminators)
        }

        quoteSentences = json.quoteSentences ?: 0
        quoteSentencesWithAttr = json.quoteSentencesWithAttr ?: 0
        attrNameWordCounts = json.attrNameWordCounts ?: []
        attrNameLengths = json.attrNameLengths ?: []
        attrTitleWordCounts = json.attrTitleWordCounts ?: []
        attrTitleLengths = json.attrTitleLengths ?: []
    }

    // --- Generation ---

    String generateWord(int targetLen) {
        lastWordNgramLevel = ngramMode  // Reset tracking

        def lenKey = targetLen.toString()
        def typeSel = lengthStartSel[lenKey]

        if (!typeSel) {
            def fused = lengthSelector.select()
            if (!fused) return "blob"
            def parts = fused.split(':')
            lenKey = parts[0]
            targetLen = lenKey.toInteger()
            typeSel = lengthStartSel[lenKey] ?: new WeightedSelector([(parts[1]): 1])
        }

        def startType = typeSel.select()
        def effLen = Math.min(targetLen, Globals.MAX_EFF_LEN).toString()

        def tokens = []
        def start = startSel[effLen]?."${startType}"?.select() ?: (startType == 'C' ? 'st' : 'a')
        tokens << start

        def prevType = startType

        for (int i = 1; i < targetLen; i++) {
            def isLast = (i == targetLen - 1)
            String tok = null
            int levelUsed = 0

            // Try trigram
            if (ngramMode >= 3 && tokens.size() >= 2) {
                def key = "${tokens[-2]}:${tokens[-1]}"
                tok = (isLast ? tgLastSel : tgInnerSel)[effLen]?."${key}"?.select()
                if (tok) levelUsed = 3
            }

            // Try bigram
            if (!tok && ngramMode >= 2 && tokens.size() >= 1) {
                def map = isLast ? bgLastSel : (i == 1 ? bgStartSel : bgInnerSel)
                tok = map[effLen]?."${tokens[-1]}"?.select()
                if (tok) levelUsed = 2
            }

            // Try unigram
            if (!tok) {
                tok = (isLast ? lastSel : innerSel)[effLen]?."${prevType}"?.select()
                if (tok) levelUsed = 1
            }

            // Fallback
            if (!tok) {
                tok = (prevType == 'C') ? 'a' : 'n'
                levelUsed = 0
            }

            // Track lowest level used
            if (levelUsed < lastWordNgramLevel) {
                lastWordNgramLevel = levelUsed
            }

            tokens << tok
            prevType = Globals.VOWELS.contains(tok[-1]) ? 'V' : 'C'
        }

        return tokens.join('')
    }

    Segment generateStructure(String type, int depth = 0) {
        def seg = new Segment(type: type)
        def chain = structChains[type]
        if (!chain) return seg

        // Pick target length from distribution
        def lens = lengthDists[type]
        if (!lens || lens.isEmpty()) return seg
        int targetLen = lens[Globals.RND.nextInt(lens.size())]
        int maxLen = (int)(lens.max() * 1.2)  // Hard cap

        def ctxDepth = Globals.STRUCT_CONTEXT_DEPTH
        def context = ['START'] * ctxDepth
        def generated = []

        while (generated.size() < maxLen) {
            def ctxKey = context.takeRight(ctxDepth).join('|')
            def sel = chain[ctxKey]
            if (!sel) break

            def next = sel.select()
            if (!next) break

            // Bias toward END as we approach/exceed target
            if (generated.size() >= targetLen) {
                if (sel.hasKey('END')) {
                    // Increasing probability of END
                    double endProb = Math.min(0.9, 0.3 + (generated.size() - targetLen) * 0.15)
                    if (Globals.RND.nextDouble() < endProb) {
                        next = 'END'
                    }
                }
            }

            if (next == 'END') break

            // Apply entity
            if (next.startsWith('WORD:')) {
                def bin = next.substring(5)
                seg.wordLengths << Utils.unbinWordLength(bin)
            } else if (next.startsWith('PAUSE:')) {
                def p = next.substring(6)
                if (seg.wordLengths) {  // Only add pause after words
                    seg.children << new Segment(type: 'PAUSE', terminator: p,
                            insertPosition: seg.wordLengths.size())
                }
            } else if (next.startsWith('SEG:')) {
                def childType = next.substring(4)

                // Enforce quote nesting rules:
                // - QUOTE can appear at top level (SENTENCE) or inside non-quote segments (PAREN, etc)
                // - SQUOTE can ONLY appear inside QUOTE (for "quote 'within' quote")
                // - No quotes inside SQUOTE
                boolean allowChild = true
                if (childType == 'QUOTE') {
                    // QUOTE not allowed inside QUOTE or SQUOTE
                    if (type == 'QUOTE' || type == 'SQUOTE') {
                        allowChild = false
                    }
                } else if (childType == 'SQUOTE') {
                    // SQUOTE ONLY allowed inside QUOTE - nowhere else
                    if (type != 'QUOTE') {
                        allowChild = false
                    }
                }

                if (allowChild && depth < 3) {
                    def child = generateStructure(childType, depth + 1)
                    child.insertPosition = seg.wordLengths.size()
                    seg.children << child

                    // Track if sentence starts with quote
                    if (depth == 0 && seg.wordLengths.isEmpty() &&
                            seg.children.size() == 1 && childType == 'QUOTE') {
                        seg.startsWithQuote = true
                    }
                }
            }

            generated << next
            context << next
        }

        return seg
    }

    String renderSegment(Segment seg, boolean isRoot = true, String trailingPause = null) {
        def sb = new StringBuilder()

        // Opening punctuation for nested segments
        if (!isRoot) {
            switch (seg.type) {
                case 'QUOTE': sb.append('"'); break
                case 'SQUOTE': sb.append("'"); break
                case 'PAREN': sb.append('('); break
                case 'BRACKET': sb.append('['); break
                case 'DASH_ASIDE': sb.append('\u2014 '); break
            }
        }

        def sortedChildren = seg.children.findAll { it.type != 'PAUSE' }
                .sort { it.insertPosition }
        def pauses = seg.children.findAll { it.type == 'PAUSE' }
                .collectEntries { [it.insertPosition, it.terminator] }
        def consumedPauses = [] as Set  // Track pauses moved inside QUOTE children

        int childIdx = 0
        seg.wordLengths.eachWithIndex { len, i ->
            // Insert children at this position
            while (childIdx < sortedChildren.size() &&
                    sortedChildren[childIdx].insertPosition == i) {
                def child = sortedChildren[childIdx]

                // For QUOTE children, check if there's a pause at this position
                // If so, pass it to be rendered inside the quote (American style)
                String pauseForQuote = null
                if (child.type == 'QUOTE' && pauses.containsKey(i)) {
                    pauseForQuote = pauses[i]
                    consumedPauses << i
                }

                sb.append(renderSegment(child, false, pauseForQuote))
                sb.append(' ')
                childIdx++
            }

            // Generate and add word
            def word = generateWord(len)
            def wordNgramLevel = lastWordNgramLevel

            // Handle uniqueness filtering if enabled
            boolean forcedAccept = false
            if (vocabulary) {
                int retries = 0
                int mode = ngramMode
                while (vocabulary.contains(word) && retries < 60) {
                    retries++
                    if (retries >= 40) mode = 1
                    else if (retries >= 20) mode = 2
                    def oldMode = ngramMode
                    ngramMode = mode
                    word = generateWord(len)
                    // Track level of THIS generation (will be final if we exit loop)
                    wordNgramLevel = lastWordNgramLevel
                    ngramMode = oldMode
                }
                if (retries >= 60) forcedAccept = true
            }

            // Apply marking if enabled - shows fallbacks from requested n-gram level
            if (markMode) {
                if (forcedAccept) {
                    // Uniqueness abandoned after max retries
                    word = "[${word}]"
                } else if (wordNgramLevel < ngramMode) {
                    // Had to fall back to lower n-gram level
                    if (wordNgramLevel <= 1) {
                        // Fell back to unigram
                        word = "{${word}}"
                    } else if (wordNgramLevel == 2) {
                        // Fell back to bigram (only visible in trigram mode)
                        word = "<${word}>"
                    }
                }
                // If wordNgramLevel == ngramMode, no marking (achieved requested level)
            }

            sb.append(word)

            // Add pause if present (and not already consumed by a QUOTE child)
            if (pauses.containsKey(i + 1) && !consumedPauses.contains(i + 1)) {
                sb.append(pauses[i + 1])
            }

            sb.append(' ')
        }

        // Remaining children at end
        while (childIdx < sortedChildren.size()) {
            sb.append(renderSegment(sortedChildren[childIdx], false))
            sb.append(' ')
            childIdx++
        }

        // Trim trailing space
        while (sb.length() > 0 && sb.charAt(sb.length() - 1) == (' ' as char)) {
            sb.deleteCharAt(sb.length() - 1)
        }

        // Closing punctuation and terminator for nested segments
        if (!isRoot) {
            switch (seg.type) {
                case 'QUOTE':
                    // Double quotes: punctuation INSIDE (American style)
                    if (seg.terminator) sb.append(seg.terminator)
                    // Also include any trailing pause from parent (comma after quote)
                    if (trailingPause) sb.append(trailingPause)
                    sb.append('"')
                    break
                case 'SQUOTE':
                    // Single quotes: punctuation OUTSIDE
                    sb.append("'")
                    if (seg.terminator) sb.append(seg.terminator)
                    break
                case 'PAREN':
                    if (seg.terminator) sb.append(seg.terminator)
                    sb.append(')')
                    break
                case 'BRACKET':
                    if (seg.terminator) sb.append(seg.terminator)
                    sb.append(']')
                    break
                case 'DASH_ASIDE':
                    if (seg.terminator) sb.append(seg.terminator)
                    sb.append(' \u2014')
                    break
                default:
                    if (seg.terminator) sb.append(seg.terminator)
            }
        }

        return sb.toString()
    }

    Attribution generateAttribution() {
        def attr = new Attribution()

        // Pick number of name words from distribution, then pick that many lengths
        if (attrNameWordCounts && attrNameLengths) {
            int nameWordCount = attrNameWordCounts[Globals.RND.nextInt(attrNameWordCounts.size())]
            nameWordCount.times {
                attr.nameLengths << attrNameLengths[Globals.RND.nextInt(attrNameLengths.size())]
            }
        } else {
            // Default: 1-2 name words of length 2-4
            (Globals.RND.nextInt(2) + 1).times { attr.nameLengths << (Globals.RND.nextInt(3) + 2) }
        }

        // Maybe add title - probability based on how many attributions have titles
        double titleProb = (quoteSentencesWithAttr > 0 && attrTitleWordCounts) ?
                (attrTitleWordCounts.size() / (double)quoteSentencesWithAttr) : 0.3

        if (Globals.RND.nextDouble() < titleProb && attrTitleWordCounts && attrTitleLengths) {
            attr.hasTitle = true
            int titleWordCount = attrTitleWordCounts[Globals.RND.nextInt(attrTitleWordCounts.size())]
            titleWordCount.times {
                attr.titleLengths << attrTitleLengths[Globals.RND.nextInt(attrTitleLengths.size())]
            }
        }

        return attr
    }

    String renderAttribution(Attribution attr) {
        def sb = new StringBuilder('\u2014')  // Em-dash attached to first word

        attr.nameLengths.eachWithIndex { len, i ->
            def word = generateWord(len)
            word = Utils.titleCase(word)
            if (i > 0) sb.append(' ')
            sb.append(word)
        }

        if (attr.hasTitle && attr.titleLengths) {
            sb.append(', ')
            attr.titleLengths.eachWithIndex { len, i ->
                def word = generateWord(len)
                if (i == 0) word = Utils.titleCase(word)
                if (i > 0) sb.append(' ')
                sb.append(word)
            }
        }

        return sb.toString()
    }

    String generate() {
        def seg = generateStructure('SENTENCE')
        def text = renderSegment(seg)

        // Smart capitalize
        text = Utils.smartCaps(text)

        // Handle attribution - only valid if:
        // 1. Sentence starts with quote
        // 2. Sentence has NO words outside the quote (the quote IS the sentence)
        // 3. There's only one top-level child (the quote itself)
        boolean canHaveAttribution = seg.startsWithQuote &&
                seg.wordLengths.isEmpty() &&
                seg.children.size() == 1 &&
                seg.children[0].type == 'QUOTE'

        if (canHaveAttribution && quoteSentencesWithAttr > 0) {
            double attrProb = quoteSentencesWithAttr / (double)quoteSentences
            if (Globals.RND.nextDouble() < attrProb) {
                def attr = generateAttribution()
                text = text + ' ' + renderAttribution(attr)
                return text  // Attribution is terminator
            }
        }

        // Add terminator
        def term = termSelector?.select() ?: '.'
        text = text + term

        return text
    }
}

// ----------------------------------------------------------------
// --- CONFIG & MAIN ---
// ----------------------------------------------------------------

@Canonical
class Config {
    boolean analyze = false
    boolean generate = false
    String wordFile = null
    String structFile = null
    String inputFile = null
    String outputFile = null
    boolean uniqueMode = false
    boolean markMode = false    // -m: Mark n-gram fallbacks
    int pruneMin = 0
    int ngramMode = 3
    int count = 20
    int contextDepth = 3
}

def parseArgs(String[] args) {
    def cfg = new Config()
    int i = 0
    while (i < args.length) {
        switch (args[i]) {
            case '-a': cfg.analyze = true; break
            case '-g': cfg.generate = true; break
            case '-w': cfg.wordFile = args[++i]; break
            case '-s': cfg.structFile = args[++i]; break
            case '-f': cfg.inputFile = args[++i]; break
            case '-o': cfg.outputFile = args[++i]; break
            case '-u': cfg.uniqueMode = true; break
            case '-m': cfg.markMode = true; break
            case '-p': cfg.pruneMin = args[++i].toInteger(); break
            case '-c': cfg.count = args[++i].toInteger(); break
            case '-d': cfg.contextDepth = args[++i].toInteger(); break
            case '-1': cfg.ngramMode = 1; break
            case '-2': cfg.ngramMode = 2; break
            case '-3': cfg.ngramMode = 3; break
        }
        i++
    }
    return cfg
}

def showUsage() {
    println """
PSEUDO TEXT GENERATOR - Fixed-Depth Markov Edition

ANALYZE:
  groovy script.groovy -a -f <input.txt> -w <words.json> [-s <struct.json>] [-u] [-d <depth>]
  
  -f  Input text file
  -w  Output word statistics JSON
  -s  Output structure statistics JSON (optional, enables sentence generation)
  -u  Track vocabulary for unique word generation
  -d  Markov context depth for structure (default: 3)

GENERATE:
  groovy script.groovy -g -w <words.json> [-s <struct.json>] [-o <output.txt>] [-c <count>] [-u] [-m] [-p <min>]
  
  -w  Input word statistics JSON
  -s  Input structure statistics JSON (optional, if omitted generates words only)
  -o  Output file (default: stdout)
  -c  Number of items to generate (default: 20)
  -u  Unique mode: avoid generating real words from vocabulary
  -m  Mark mode: show n-gram fallback markers
      <word> = backed off from trigram to bigram
      {word} = backed off from bigram to unigram  
      [word] = all n-grams failed, accepted existing word
  -p  Prune: minimum word length to generate
  -1/-2/-3  N-gram mode for word generation (default: 3)
"""
}

// --- MAIN ---

def cfg = parseArgs(args)
Globals.STRUCT_CONTEXT_DEPTH = cfg.contextDepth

if (cfg.analyze && cfg.generate) {
    println "Error: Choose either -a (analyze) or -g (generate), not both."
    showUsage()
    System.exit(1)
}

if (!cfg.analyze && !cfg.generate) {
    showUsage()
    System.exit(0)
}

if (cfg.analyze) {
    if (!cfg.inputFile || !cfg.wordFile) {
        println "Error: Analysis requires -f <input> and -w <output>"
        System.exit(1)
    }

    println "=== ANALYSIS ==="
    println "Input: ${cfg.inputFile}"
    println "Word stats: ${cfg.wordFile}"
    if (cfg.structFile) println "Structure stats: ${cfg.structFile}"
    println "Context depth: ${cfg.contextDepth}"
    println ""

    def text = new File(cfg.inputFile).getText('UTF-8')
    def preprocessor = new Preprocessor()
    def processed = preprocessor.process(text)

    def tokenizer = new Tokenizer()
    def tokens = tokenizer.tokenize(processed)

    println "Tokens: ${tokens.size()}"
    println "Words: ${tokens.count { it.type == 'WORD' }}"

    // Word analysis
    def wordAnalyzer = new WordAnalyzer()
    wordAnalyzer.analyze(tokens, cfg.uniqueMode)

    def wordJson = JsonOutput.prettyPrint(JsonOutput.toJson(wordAnalyzer.toJson(cfg.uniqueMode)))
    Files.write(Paths.get(cfg.wordFile), wordJson.getBytes('UTF-8'))
    println "Word stats saved: ${cfg.wordFile}"

    if (cfg.uniqueMode) {
        println "Vocabulary: ${wordAnalyzer.vocabulary.size()} unique words"
    }

    // Structure analysis
    if (cfg.structFile) {
        def parser = new StructureParser()
        def segments = parser.parse(tokens)

        println "Sentences: ${segments.size()}"

        // Count quote-starting sentences and attributions
        int quoteStarts = segments.count { it.startsWithQuote }
        int withAttr = segments.count { it.attribution != null }
        println "Quote-starting sentences: ${quoteStarts}"
        println "Sentences with attribution: ${withAttr}"

        def markov = new MarkovAnalyzer()
        markov.analyze(segments)

        def structJson = JsonOutput.prettyPrint(JsonOutput.toJson(markov.toJson()))
        Files.write(Paths.get(cfg.structFile), structJson.getBytes('UTF-8'))
        println "Structure stats saved: ${cfg.structFile}"

        if (markov.quoteSentences > 0) {
            println "Quote sentences (from analyzer): ${markov.quoteSentences} (${markov.quoteSentencesWithAttr} with attribution)"
        }
    }

    println "\nDone."
}

if (cfg.generate) {
    if (!cfg.wordFile) {
        println "Error: Generation requires -w <word stats>"
        System.exit(1)
    }

    println "=== GENERATION ==="
    println "Word stats: ${cfg.wordFile}"
    if (cfg.structFile) println "Structure stats: ${cfg.structFile}"
    println "Count: ${cfg.count}"
    println "N-gram mode: ${cfg.ngramMode}"
    if (cfg.uniqueMode) println "Unique mode: ON"
    if (cfg.markMode) println "Mark mode: ON"
    println ""

    def gen = new Generator()
    gen.ngramMode = cfg.ngramMode
    gen.markMode = cfg.markMode
    gen.loadWordModel(cfg.wordFile, cfg.pruneMin, cfg.uniqueMode)

    if (cfg.structFile) {
        gen.loadStructModel(cfg.structFile)
        println "Attribution stats: ${gen.quoteSentences} quote sentences, ${gen.quoteSentencesWithAttr} with attribution"
        if (gen.quoteSentences > 0) {
            println "Attribution probability: ${String.format('%.1f', 100.0 * gen.quoteSentencesWithAttr / gen.quoteSentences)}%"
        }
        println ""
    }

    if (cfg.uniqueMode && !gen.vocabulary) {
        println "Warning: Unique mode requires vocabulary in word stats (use -u during analysis)"
    }

    def output = cfg.outputFile ? new PrintStream(new File(cfg.outputFile)) : System.out

    if (!cfg.outputFile) println "--- OUTPUT ---"

    cfg.count.times {
        if (cfg.structFile) {
            output.println(gen.generate())
        } else {
            def fused = gen.lengthSelector.select()
            if (fused) {
                def len = fused.split(':')[0].toInteger()
                output.println(gen.generateWord(len))
            }
        }
    }

    if (cfg.outputFile) {
        output.close()
        println "Output saved: ${cfg.outputFile}"
    }

    println "\nDone."
}