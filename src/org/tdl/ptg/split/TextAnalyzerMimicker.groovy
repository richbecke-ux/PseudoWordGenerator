package org.tdl.ptg.split

import groovy.transform.Canonical
import groovy.json.JsonSlurper
import groovy.json.JsonOutput
import java.nio.file.Files
import java.nio.file.Paths

// ----------------------------------------------------------------
// --- SHARED GLOBALS ---
// ----------------------------------------------------------------

class Globals {
    static final Random RND = new Random()
    static final Set<String> VOWELS = new HashSet(['a', 'e', 'i', 'o', 'u', 'y', 'æ', 'ø', 'å', 'ä', 'ö', 'ü', 'é'])
    static final Set<String> TERMINATORS = new HashSet(['.', '!', '?', '\u2026'])
    static final Set<String> PAUSES = new HashSet([',', ';', ':'])
    static final int MAX_EFF_LEN = 8
}

// ----------------------------------------------------------------
// --- TOKEN DEFINITIONS ---
// ----------------------------------------------------------------

@Canonical
class Token {
    String type      // WORD, TERM, PAUSE, OPEN, CLOSE, TOGGLE, OPEN_SQUOTE, CLOSE_SQUOTE, HYPHEN
    String text = ''
    String value = ''
    String context = '' // QUOTE, PAREN, DASH, SENTENCE, HYPHEN
    int wordLength = 0
}

class Utils {
    static List tokenizeStructure(String word) {
        // STRICT: Only letters. Numbers are ignored.
        def cleanWord = word.findAll { Character.isLetter(it as char) }.join('').toLowerCase()
        if (!cleanWord) return []
        def tokens = []
        def curType = ''
        def curContent = new StringBuilder()
        cleanWord.each { c ->
            def charStr = c.toString()
            def type = Globals.VOWELS.contains(charStr) ? 'V' : 'C'
            if (curType == '' || curType == type) { curType = type; curContent.append(charStr) }
            else {
                tokens << [curType, curContent.toString()]
                curType = type
                curContent = new StringBuilder().append(charStr)
            }
        }
        if (curContent) tokens << [curType, curContent.toString()]
        return tokens
    }

    static def ensureMap(Map map, String k1, String k2) {
        map.computeIfAbsent(k1, { [:] }).computeIfAbsent(k2, { [:] })
    }

    static String smartCaps(String s) {
        if (!s) return s
        StringBuilder sb = new StringBuilder(s)
        boolean inAnsi = false

        for (int i = 0; i < sb.length(); i++) {
            char c = sb.charAt(i)

            // Detect start of ANSI sequence
            if (c == '\u001B') {
                inAnsi = true
                continue
            }

            if (inAnsi) {
                // ANSI SGR sequences end with 'm' (usually)
                // We blindly skip everything until we hit the terminator
                if (c == 'm') {
                    inAnsi = false
                }
                continue
            }

            // Found a visible letter? Capitalize and stop.
            if (Character.isLetter(c)) {
                sb.setCharAt(i, Character.toUpperCase(c))
                break
            }
        }
        return sb.toString()
    }
}

// ----------------------------------------------------------------
// --- PARSER MODELS ---
// ----------------------------------------------------------------

@Canonical
class Segment {
    String type = 'SENTENCE' // SENTENCE, QUOTE, SQUOTE, PAREN, DASH, HYPHEN
    List<Integer> wordLengths = []
    List<Segment> children = []
    int childInsertPosition = -1
    String terminator = ''
    String debugText = ''
}

// ----------------------------------------------------------------
// --- TOKENIZER ---
// ----------------------------------------------------------------

class Tokenizer {

    int quoteMode = 2  // Default to simpler mode; can be set to 1 for stricter validation

    boolean isBoundary(char c) {
        return Character.isWhitespace(c) || Globals.TERMINATORS.contains(c.toString()) || Globals.PAUSES.contains(c.toString()) || c == ')' || c == ']'
    }

    boolean isConsonantOnly(String word) {
        if (!word) return false
        String lower = word.toLowerCase()
        return lower.every { String c ->
            Character.isLetter(c as char) && !Globals.VOWELS.contains(c)
        }
    }

    List<Token> tokenize(String text) {
        List<Token> tokens = []
        StringBuilder buffer = new StringBuilder()

        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i)
            String s = c.toString()

            // Acronym Logic - IMPROVED
            boolean isAcronymDot = false
            if (s == '.') {
                boolean prevIsAlpha = buffer.length() > 0 && Character.isLetter(buffer.charAt(buffer.length() - 1))
                boolean nextIsAlpha = (i < text.length() - 1) && Character.isLetter(text.charAt(i+1))

                // Standard acronym: U.S.A.
                if (prevIsAlpha && nextIsAlpha) {
                    isAcronymDot = true
                }
                // Repeated dots: U.S.
                else if (prevIsAlpha && buffer.length() >= 2 && buffer.charAt(buffer.length() - 2) == '.') {
                    isAcronymDot = true
                }
                // Consonant-only cluster: Mr. Mrs. Dr. vs. etc.
                else if (prevIsAlpha && isConsonantOnly(buffer.toString())) {
                    String bufferLower = buffer.toString().toLowerCase()
                    if (bufferLower == 'etc') {
                        // "etc." is only an abbreviation if followed by comma, colon, semicolon, dash, or more text
                        boolean nextIsPunctContinuation = (i < text.length() - 1) &&
                                [',', ';', ':', '-'].contains(text.charAt(i+1).toString())
                        boolean hasMoreText = (i < text.length() - 1) && !Character.isWhitespace(text.charAt(i+1))
                        isAcronymDot = nextIsPunctContinuation || (hasMoreText && !nextIsPunctContinuation)
                    } else {
                        // All other consonant clusters are abbreviations
                        isAcronymDot = true
                    }
                }
            }

            if (s == '\'') {
                if (quoteMode == 2) {
                    // MODE 2: Simpler tokenizer-level fix
                    boolean prevIsSpace = (i == 0) || isBoundary(text.charAt(i-1))
                    boolean nextIsAlpha = (i < text.length() - 1) && Character.isLetterOrDigit(text.charAt(i+1))
                    boolean prevIsAlpha = (i > 0) && !isBoundary(text.charAt(i-1))
                    boolean nextIsSpace = (i == text.length() - 1) || isBoundary(text.charAt(i+1))

                    // Opening single quote: must be preceded by space AND followed by uppercase letter
                    // This avoids contractions like 'way, 'round which are lowercase
                    boolean isLikelyOpening = prevIsSpace && nextIsAlpha &&
                            Character.isUpperCase(text.charAt(i+1))

                    // Closing single quote: preceded by alpha, followed by boundary
                    boolean isLikelyClosing = prevIsAlpha && nextIsSpace

                    if (isLikelyOpening) {
                        if (buffer.length() > 0) { tokens << createWord(buffer); buffer = new StringBuilder() }
                        tokens << new Token(type: 'OPEN_SQUOTE', value: "'", context: 'SQUOTE')
                    }
                    else if (isLikelyClosing) {
                        if (buffer.length() > 0) { tokens << createWord(buffer); buffer = new StringBuilder() }
                        tokens << new Token(type: 'CLOSE_SQUOTE', value: "'", context: 'SQUOTE')
                    }
                    else {
                        // Everything else: part of word (possessives, contractions, etc.)
                        buffer.append(c)
                    }
                } else {
                    // MODE 1: Original logic - validation happens in parser
                    boolean prevIsSpace = (i == 0) || isBoundary(text.charAt(i-1))
                    boolean nextIsAlpha = (i < text.length() - 1) && Character.isLetterOrDigit(text.charAt(i+1))
                    boolean prevIsAlpha = (i > 0) && !isBoundary(text.charAt(i-1))
                    boolean nextIsSpace = (i == text.length() - 1) || isBoundary(text.charAt(i+1))

                    if (prevIsSpace && nextIsAlpha) {
                        if (buffer.length() > 0) { tokens << createWord(buffer); buffer = new StringBuilder() }
                        tokens << new Token(type: 'OPEN_SQUOTE', value: "'", context: 'SQUOTE')
                    }
                    else if (prevIsAlpha && nextIsSpace) {
                        if (buffer.length() > 0) { tokens << createWord(buffer); buffer = new StringBuilder() }
                        tokens << new Token(type: 'CLOSE_SQUOTE', value: "'", context: 'SQUOTE')
                    }
                    else {
                        buffer.append(c)
                    }
                }
            }
            else if (isAcronymDot) {
                buffer.append(c)
            }
            else if (s == '-') {
                boolean prevIsAlpha = (i > 0) && Character.isLetter(text.charAt(i-1))
                boolean nextIsAlpha = (i < text.length() - 1) && Character.isLetter(text.charAt(i+1))
                if (prevIsAlpha && nextIsAlpha) {
                    if (buffer.length() > 0) { tokens << createWord(buffer); buffer = new StringBuilder() }
                    tokens << new Token(type: 'HYPHEN', value: '-', context: 'HYPHEN')
                } else {
                    if (buffer.length() > 0) { tokens << createWord(buffer); buffer = new StringBuilder() }
                }
            }
            else if (s == '"') {
                if (buffer.length() > 0) { tokens << createWord(buffer); buffer = new StringBuilder() }
                tokens << new Token(type: 'TOGGLE', value: '"', context: 'QUOTE')
            }
            else if (s == '\u2014') {
                if (buffer.length() > 0) { tokens << createWord(buffer); buffer = new StringBuilder() }
                tokens << new Token(type: 'TOGGLE', value: '\u2014', context: 'DASH')
            }
            else if (s == '(' || s == '[') {
                if (buffer.length() > 0) { tokens << createWord(buffer); buffer = new StringBuilder() }
                tokens << new Token(type: 'OPEN', value: '(', context: 'PAREN')
            }
            else if (s == ')' || s == ']') {
                if (buffer.length() > 0) { tokens << createWord(buffer); buffer = new StringBuilder() }
                tokens << new Token(type: 'CLOSE', value: ')', context: 'PAREN')
            }
            else if (Globals.TERMINATORS.contains(s)) {
                if (buffer.length() > 0) { tokens << createWord(buffer); buffer = new StringBuilder() }
                tokens << new Token(type: 'TERM', value: s, context: 'SENTENCE')
            }
            else if (Globals.PAUSES.contains(s)) {
                if (buffer.length() > 0) { tokens << createWord(buffer); buffer = new StringBuilder() }
                tokens << new Token(type: 'PAUSE', value: s)
            }
            else if (Character.isWhitespace(c)) {
                if (buffer.length() > 0) { tokens << createWord(buffer); buffer = new StringBuilder() }
            }
            else {
                buffer.append(c)
            }
        }
        if (buffer.length() > 0) tokens << createWord(buffer)
        return tokens
    }

    Token createWord(StringBuilder sb) {
        String w = sb.toString()
        def struct = Utils.tokenizeStructure(w)
        if (!struct) return null
        return new Token(type: 'WORD', text: w, wordLength: struct.size())
    }
}

// ----------------------------------------------------------------
// --- PARSER ---
// ----------------------------------------------------------------

class Parser {

    int quoteMode = 2  // Default to simpler mode; can be set to 1 for stricter validation

    boolean stackHasType(List<Segment> stack, String type) {
        return stack.any { it.type == type }
    }

    boolean isValidSingleQuoteSegment(List<Token> tokens, int startIdx, int endIdx) {
        // Scan tokens between opening and closing single quote
        for (int i = startIdx + 1; i < endIdx; i++) {
            Token t = tokens[i]

            // Period (sentence terminator) inside single quotes = invalid
            if (t.type == 'TERM') {
                return false
            }

            // Em-dash inside single quotes = invalid
            if (t.type == 'TOGGLE' && t.context == 'DASH') {
                return false
            }

            // Double quote inside single quotes = invalid
            if (t.type == 'TOGGLE' && t.context == 'QUOTE') {
                return false
            }
        }
        return true
    }

    List<Segment> parse(List<Token> tokens) {
        List<Segment> completed = []
        List<Segment> stack = [new Segment(type: 'SENTENCE')]

        for (int i = 0; i < tokens.size(); i++) {
            Token tok = tokens[i]
            if (stack.isEmpty()) stack.add(new Segment(type: 'SENTENCE'))

            String visual = tok.text ?: tok.value
            if (visual) stack.first().debugText += visual + " "

            Segment current = stack.last()

            switch (tok.type) {
                case 'WORD':
                    current.wordLengths << tok.wordLength
                    break

                case 'PAUSE':
                    current.children << new Segment(type: 'PAUSE', terminator: tok.value, childInsertPosition: current.wordLengths.size())
                    break

                case 'HYPHEN':
                    current.children << new Segment(type: 'HYPHEN', terminator: '-', childInsertPosition: current.wordLengths.size())
                    break

                case 'OPEN':
                    if (!stackHasType(stack, tok.context)) {
                        Segment child = new Segment(type: tok.context, childInsertPosition: current.wordLengths.size())
                        current.children << child
                        stack.add(child)
                    }
                    break

                case 'CLOSE':
                    // AUTO-CLOSE DASH: If we are in a dash inside a paren, and paren closes, pop dash.
                    if (current.type == 'DASH' && stack.size() > 1 && stack[stack.size()-2].type == tok.context) {
                        stack.removeLast()
                        current = stack.last()
                    }

                    if (current.type == tok.context) {
                        stack.removeLast()
                        if (current.terminator && Globals.TERMINATORS.contains(current.terminator)) {
                            if (!stack.isEmpty() && stack.last().type == 'SENTENCE') {
                                completed << stack.first()
                                stack.clear()
                                stack.add(new Segment(type: 'SENTENCE'))
                            }
                        }
                    }
                    break

                case 'OPEN_SQUOTE':
                    if (quoteMode == 1) {
                        // MODE 1: Stricter validation
                        // Look ahead to find matching close or invalidation
                        int closeIdx = -1
                        boolean isValid = true

                        for (int j = i + 1; j < tokens.size(); j++) {
                            Token ahead = tokens[j]

                            // Found potential closing quote
                            if (ahead.type == 'CLOSE_SQUOTE') {
                                closeIdx = j
                                break
                            }

                            // Invalid content before close - this is a contraction/apostrophe
                            if (ahead.type == 'TERM' ||
                                    (ahead.type == 'TOGGLE' && ahead.context == 'DASH') ||
                                    (ahead.type == 'TOGGLE' && ahead.context == 'QUOTE')) {
                                isValid = false
                                break
                            }
                        }

                        if (!isValid || closeIdx == -1) {
                            // This is not actually a single quote - skip it (treat as contraction)
                            break
                        }

                        // Valid single quote - proceed with normal handling
                        if (!stackHasType(stack, 'SQUOTE')) {
                            Segment child = new Segment(type: 'SQUOTE', childInsertPosition: current.wordLengths.size())
                            current.children << child
                            stack.add(child)
                        }
                    } else {
                        // MODE 2: Original handling (already filtered by tokenizer)
                        if (!stackHasType(stack, 'SQUOTE')) {
                            Segment child = new Segment(type: 'SQUOTE', childInsertPosition: current.wordLengths.size())
                            current.children << child
                            stack.add(child)
                        }
                    }
                    break

                case 'CLOSE_SQUOTE':
                    // AUTO-CLOSE DASH
                    if (current.type == 'DASH' && stack.size() > 1 && stack[stack.size()-2].type == 'SQUOTE') {
                        stack.removeLast()
                        current = stack.last()
                    }

                    if (current.type == 'SQUOTE') {
                        stack.removeLast()
                        if (current.terminator && Globals.TERMINATORS.contains(current.terminator)) {
                            if (!stack.isEmpty() && stack.last().type == 'SENTENCE') {
                                completed << stack.first()
                                stack.clear(); stack.add(new Segment(type: 'SENTENCE'))
                            }
                        }
                    }
                    break

                case 'TOGGLE':
                    // AUTO-CLOSE DASH
                    if (current.type == 'DASH' && stack.size() > 1 && stack[stack.size()-2].type == tok.context) {
                        stack.removeLast()
                        current = stack.last()
                    }

                    if (current.type == tok.context) {
                        stack.removeLast()
                        if (current.terminator && Globals.TERMINATORS.contains(current.terminator)) {
                            if (!stack.isEmpty() && stack.last().type == 'SENTENCE') {
                                completed << stack.first()
                                stack.clear()
                                stack.add(new Segment(type: 'SENTENCE'))
                            }
                        }
                    }
                    else if (!stackHasType(stack, tok.context)) {
                        Segment child = new Segment(type: tok.context, childInsertPosition: current.wordLengths.size())
                        current.children << child
                        stack.add(child)
                    }
                    break

                case 'TERM':
                    if (current.type == 'SENTENCE') {
                        current.terminator = tok.value
                        completed << stack.first()
                        stack.clear(); stack.add(new Segment(type: 'SENTENCE'))
                    }
                    else if (current.type == 'DASH') {
                        current.terminator = tok.value
                        stack.removeLast()
                        if (!stack.isEmpty() && stack.last().type == 'SENTENCE') {
                            stack.last().terminator = tok.value
                            completed << stack.first()
                            stack.clear(); stack.add(new Segment(type: 'SENTENCE'))
                        }
                    }
                    else if (current.type == 'QUOTE' || current.type == 'SQUOTE' || current.type == 'PAREN') {
                        current.terminator = tok.value
                    }
                    break
            }
        }
        if (stack.size() > 0 && (stack.first().wordLengths || stack.first().children)) {
            completed << stack.first()
        }
        return completed
    }
}

// ----------------------------------------------------------------
// --- CONFIGURATION ---
// ----------------------------------------------------------------

@Canonical
class Config {
    boolean analyze = false
    boolean generate = false
    boolean bracketMode = false  // -m: bracket fallbacks
    boolean colorMode = false    // -c: color by generation method
    boolean lineOutput = false
    boolean debug = false
    String wordStatsFile = null
    String sentenceStatsFile = null
    String f_File = null
    boolean vocabFilter = false  // -v: vocabulary filtering
    boolean uppercaseMode = false  // -u: uppercase first char
    int pruneMinTokens = 0
    int ngramMode = 1  // Default to unigram
    int quoteMode = 2  // Default to mode 2 (simpler)
    int itemCount = 20  // Renamed from 'count'
    boolean valid = false
    String error = ""

    String toString() {
        if (analyze) {
            return """
=== ANALYSIS CONFIGURATION ===
Input File        : ${f_File}
Word Stats Out    : ${wordStatsFile}
Sentence Stats Out: ${sentenceStatsFile ?: '(None)'}
Vocab Filter      : ${vocabFilter ? 'Active (Saving vocabulary list)' : 'Inactive'}
Quote Mode        : ${quoteMode} (${quoteMode == 1 ? 'Strict Validation' : 'Simple Tokenizer'})
Debug Mode        : ${debug ? 'Active (Printing Parse Tree)' : 'Inactive'}
=============================="""
        } else {
            def markingDesc = []
            if (bracketMode) markingDesc << 'Fallback Brackets'
            if (colorMode) markingDesc << 'N-Gram Coloring'
            def markingStr = markingDesc.isEmpty() ? 'Inactive' : markingDesc.join(' + ')
            def uppercaseDesc = uppercaseMode ? 'Active (Capitalize First Char)' : 'Inactive'
            return """
=== GENERATION CONFIGURATION ===
Word Stats In     : ${wordStatsFile}
Sentence Stats In : ${sentenceStatsFile ?: '(None) - Word List Mode'}
Output File       : ${f_File ?: 'STDOUT'}
Output Format     : ${lineOutput ? 'Line-by-Line (-l)' : 'Space-Separated (Default)'}
N-Gram Mode       : ${ngramMode} (${ ngramMode == 1 ? 'Unigram - Default' : ngramMode == 2 ? 'Bigram (-b)' : 'Trigram (-t)'})
Vocab Filter      : ${vocabFilter ? 'Active (Fallback: Tri->Bi->Uni)' : 'Inactive'}
Uppercase Mode    : ${uppercaseDesc}
Marking Mode      : ${markingStr}
Prune Mode        : ${pruneMinTokens > 0 ? "Active (excluding ≤ ${pruneMinTokens} tokens)" : 'Inactive'}
Item Count        : ${itemCount}
================================"""
        }
    }
}

def parseArgs(String[] args) {
    def cfg = new Config()

    // Handle help flag first
    if (args.length == 0 || args.contains('-h') || args.contains('--help')) {
        cfg.error = "Help requested"
        return cfg
    }

    def i = 0
    while (i < args.length) {
        def arg = args[i]
        switch (arg) {
            case '-a': cfg.analyze = true; break
            case '-g': cfg.generate = true; break
            case '-m': cfg.bracketMode = true; break  // Bracket fallbacks
            case '-c': cfg.colorMode = true; break    // Color by n-gram
            case '-l': cfg.lineOutput = true; break
            case '-d': cfg.debug = true; break
            case '-w': cfg.wordStatsFile = args[++i]; break
            case '-s': cfg.sentenceStatsFile = args[++i]; break
            case '-f': cfg.f_File = args[++i]; break
            case '-v': cfg.vocabFilter = true; break  // Vocab filter
            case '-u': cfg.uppercaseMode = true; break  // Uppercase mode
            case '-p': cfg.pruneMinTokens = args[++i].toInteger(); break
            case '-n': cfg.itemCount = args[++i].toInteger(); break  // Renamed from -c
            case '-q': cfg.quoteMode = args[++i].toInteger(); break
            case '-b': cfg.ngramMode = 2; break  // Bigram
            case '-t': cfg.ngramMode = 3; break  // Trigram
            default:
                cfg.error = "Unknown option: ${arg}"
                return cfg
        }
        i++
    }
    if (cfg.analyze && cfg.generate) { cfg.error = "Ambiguous Mode: Select either -a or -g."; return cfg }
    if (!cfg.analyze && !cfg.generate) { cfg.error = "No Mode Specified: Use -a or -g."; return cfg }
    if (cfg.quoteMode != 1 && cfg.quoteMode != 2) { cfg.error = "Invalid quote mode: Use -q 1 or -q 2."; return cfg }
    if (cfg.uppercaseMode && cfg.sentenceStatsFile) { cfg.error = "Uppercase mode (-u) only valid in word list mode (no -s)."; return cfg }
    cfg.valid = true
    return cfg
}

@Canonical
class WeightedSelector {
    List<String> keys = []
    List<Long> cumulativeWeights = []
    long totalWeight = 0
    WeightedSelector(Map<String, Integer> map) {
        long runningTotal = 0L
        map.sort { -it.value }.each { key, weight ->
            if (weight > 0) { this.keys.add(key); runningTotal += weight; this.cumulativeWeights.add(runningTotal) }
        }
        this.totalWeight = runningTotal
    }
    String select(Random rnd) {
        if (totalWeight <= 0) return null
        long target = (long) (rnd.nextDouble() * totalWeight) + 1
        int index = Collections.binarySearch(cumulativeWeights, target)
        if (index < 0) index = -index - 1
        if (index >= keys.size()) index = keys.size() - 1
        return keys[index]
    }
}

// ----------------------------------------------------------------
// --- MARKOV ANALYZER ---
// ----------------------------------------------------------------

class StructuralMarkovAnalyzer {
    Map<String, Map<String, Integer>> markovChain = [:]
    Map<String, List<Integer>> lengthStats = [:]
    Map<String, List<String>> segmentStarts = [:]

    int contextDepth = 3

    String binWordLen(int len) {
        if (len <= 3) return "SHORT"
        if (len <= 7) return "MED"
        return "LONG"
    }

    String encodeEntity(String type, String val) { return "${type}:${val}" }

    void train(List<Segment> parsedSegments) {
        parsedSegments.each { rootSeg -> processSegment(rootSeg) }
    }

    void processSegment(Segment seg) {
        int entityCount = seg.wordLengths.size() + seg.children.size()
        lengthStats.computeIfAbsent(seg.type, {[]}).add(entityCount)

        Map<Integer, String> streamMap = [:]
        seg.wordLengths.eachWithIndex { len, i -> streamMap[i] = encodeEntity("WORD", binWordLen(len)) }
        seg.children.each { child ->
            processSegment(child)
            String val = ''
            if (child.type == 'PAUSE') val = child.terminator ?: ','
            else if (child.type == 'HYPHEN') val = '-'
            else val = child.type

            streamMap[child.childInsertPosition] = encodeEntity((child.type == 'PAUSE' || child.type == 'HYPHEN' ? 'PAUSE' : 'SEGMENT'), val)
        }

        List<String> stream = []
        streamMap.sort { it.key }.each { k, v -> stream << v }
        updateChain(seg.type, stream)
    }

    void updateChain(String segType, List<String> stream) {
        if (stream.isEmpty()) return
        def startCtx = (0..<Math.min(stream.size(), contextDepth)).collect { stream[it] }.join("|")
        segmentStarts.computeIfAbsent(segType, {[]}).add(startCtx)
        for (int i = 0; i < stream.size(); i++) {
            def contextList = []
            for (int j = 1; j <= contextDepth; j++) {
                if (i - j >= 0) contextList.add(0, stream[i-j])
                else contextList.add(0, "START")
            }
            def key = "${segType}|" + contextList.join("|")
            def next = stream[i]
            markovChain.computeIfAbsent(key, {[:]}).merge(next, 1, Integer::sum)
        }
        def lastCtx = []
        for (int j = 0; j < contextDepth; j++) {
            def idx = stream.size() - 1 - j
            if (idx >= 0) lastCtx.add(0, stream[idx])
            else lastCtx.add(0, "START")
        }
        def endKey = "${segType}|" + lastCtx.join("|")
        markovChain.computeIfAbsent(endKey, {[:]}).merge("END", 1, Integer::sum)
    }
}

// ----------------------------------------------------------------
// --- ANALYZER ---
// ----------------------------------------------------------------

class Analyzer {
    Tokenizer tokenizer = new Tokenizer()
    Parser parser = new Parser()
    StructuralMarkovAnalyzer markovAnalyzer = new StructuralMarkovAnalyzer()

    Map<String, Integer> lengthStartStats = [:]
    Map<String, Map<String, Map<String, Integer>>> startTokens = [:]
    Map<String, Map<String, Map<String, Integer>>> innerTokens = [:]
    Map<String, Map<String, Map<String, Integer>>> lastTokens = [:]
    Map<String, Map<String, Map<String, Integer>>> bigramStart = [:]
    Map<String, Map<String, Map<String, Integer>>> bigramInner = [:]
    Map<String, Map<String, Map<String, Integer>>> bigramLast = [:]
    Map<String, Map<String, Map<String, Integer>>> trigramInner = [:]
    Map<String, Map<String, Map<String, Integer>>> trigramLast = [:]
    Set<String> vocabulary = new HashSet<>()
    Map<String, Map<String, Integer>> transitions = [:]

    void debugSegments(List<Segment> segments) {
        println "\n🐛 DEBUG: Parsed Segments Tree:"
        segments.each { seg ->
            println "SOURCE: " + seg.debugText.trim()
            printSegment(seg, 0)
            println "-" * 40
        }
        println "🐛 END DEBUG\n"
    }

    void printSegment(Segment seg, int depth) {
        String indent = "  " * depth
        String info = "${seg.type} [Words: ${seg.wordLengths}]"
        if (seg.terminator) info += " [Term: '${seg.terminator}']"
        println "${indent}${info}"

        seg.children.each { child ->
            if (child.type == 'PAUSE') {
                println "${indent}  -> PAUSE '${child.terminator}' (at idx ${child.childInsertPosition})"
            } else if (child.type == 'HYPHEN') {
                println "${indent}  -> HYPHEN '-' (at idx ${child.childInsertPosition})"
            } else {
                println "${indent}  -> CHILD (at idx ${child.childInsertPosition}):"
                printSegment(child, depth + 2)
            }
        }
    }

    String cleanText(String text) {
        if (!text) return text
        if (text.length() > 0 && text.charAt(0) == '\uFEFF') text = text.substring(1)

        // Normalize using Unicode Escapes to avoid encoding issues
        // \u201C (Left Double), \u201D (Right Double) -> "
        text = text.replaceAll(/[\u201C\u201D]/, '"')
        // \u2018 (Left Single), \u2019 (Right Single) -> '
        text = text.replaceAll(/[\u2018\u2019]/, "'")

        text = text.replaceAll(/--+/, '\u2014')
        text = text.replaceAll(/\r?\n\s*\r?\n/, " . ")
        text = text.replaceAll(/\r?\n/, " ")
        text = text.trim()

        // Hygiene: Aggressively strip wrapper quotes
        boolean stripped = true
        while (stripped && text.length() > 2) {
            stripped = false
            if (text.startsWith('"') && text.endsWith('"')) {
                text = text.substring(1, text.length() - 1).trim()
                stripped = true
            }
        }
        return text
    }

    void process(Config cfg) {
        // Set quote mode for both tokenizer and parser
        tokenizer.quoteMode = cfg.quoteMode
        parser.quoteMode = cfg.quoteMode

        println "📖 Analyzing source..."
        def rawText = new File(cfg.f_File).getText("UTF-8")
        def text = cleanText(rawText)

        List<Token> tokens = tokenizer.tokenize(text)
        tokens = tokens.findAll { it != null }

        int wordsProcessed = 0
        tokens.findAll { it.type == 'WORD' && it.wordLength > 0 }.each { tok ->
            def clean = tok.text.findAll { Character.isLetter(it as char) }.join('').toLowerCase()
            def struct = Utils.tokenizeStructure(tok.text)
            if (struct) {
                wordsProcessed++
                if (cfg.vocabFilter) vocabulary.add(clean)
                def len = struct.size()
                def effLen = Math.min(len, Globals.MAX_EFF_LEN).toString()
                def startType = struct[0][0]
                def fusedKey = "${len}:${startType}"
                lengthStartStats[fusedKey] = (lengthStartStats[fusedKey] ?: 0) + 1

                struct.eachWithIndex { t, idx ->
                    def content = t[1]
                    if (idx == 0) {
                        Utils.ensureMap(startTokens, effLen, startType)[content] = (Utils.ensureMap(startTokens, effLen, startType)[content] ?: 0) + 1
                    } else {
                        def prev = struct[idx-1]; def prevContent = prev[1]; def prevType = prev[0]
                        def baseMap = (idx == len - 1) ? lastTokens : innerTokens
                        Utils.ensureMap(baseMap, effLen, prevType)[content] = (Utils.ensureMap(baseMap, effLen, prevType)[content] ?: 0) + 1
                        def bgMap = (idx == len - 1) ? bigramLast : (idx == 1) ? bigramStart : bigramInner
                        Utils.ensureMap(bgMap, effLen, prevContent)[content] = (Utils.ensureMap(bgMap, effLen, prevContent)[content] ?: 0) + 1
                        if (idx >= 2) {
                            def prevPrev = struct[idx-2][1]
                            def tgKey = "${prevPrev}:${prevContent}"
                            def tgMap = (idx == len - 1) ? trigramLast : trigramInner
                            Utils.ensureMap(tgMap, effLen, tgKey)[content] = (Utils.ensureMap(tgMap, effLen, tgKey)[content] ?: 0) + 1
                        }
                    }
                }
            }
        }
        println "✅ Words analyzed: ${wordsProcessed}"

        if (cfg.sentenceStatsFile) {
            List<Segment> parsedSegments = parser.parse(tokens)
            if (cfg.debug) debugSegments(parsedSegments)

            parsedSegments.each { rootSeg ->
                if (rootSeg.terminator) transitions["TERM"] = (transitions["TERM"] ?: [:]).tap { it[rootSeg.terminator] = (it[rootSeg.terminator] ?: 0) + 1 }
            }

            markovAnalyzer.train(parsedSegments)
            println "✅ Sentences analyzed: ${parsedSegments.size()}"
        }
    }

    void save(Config cfg) {
        def wordData = [
                lengthStats: lengthStartStats,
                startTokens: startTokens.collectEntries { k, v -> [k, v.collectEntries { k2, v2 -> [k2, v2] }] },
                innerTokens: innerTokens.collectEntries { k, v -> [k, v.collectEntries { k2, v2 -> [k2, v2] }] },
                lastTokens: lastTokens.collectEntries { k, v -> [k, v.collectEntries { k2, v2 -> [k2, v2] }] },
                bigramStart: bigramStart.collectEntries { k, v -> [k, v.collectEntries { k2, v2 -> [k2, v2] }] },
                bigramInner: bigramInner.collectEntries { k, v -> [k, v.collectEntries { k2, v2 -> [k2, v2] }] },
                bigramLast: bigramLast.collectEntries { k, v -> [k, v.collectEntries { k2, v2 -> [k2, v2] }] },
                trigramInner: trigramInner.collectEntries { k, v -> [k, v.collectEntries { k2, v2 -> [k2, v2] }] },
                trigramLast: trigramLast.collectEntries { k, v -> [k, v.collectEntries { k2, v2 -> [k2, v2] }] }
        ]
        if (cfg.vocabFilter) wordData.vocabulary = vocabulary.toList()

        new File(cfg.wordStatsFile).write(JsonOutput.prettyPrint(JsonOutput.toJson(wordData)))
        println "💾 Saved word stats to ${cfg.wordStatsFile}"

        if (cfg.sentenceStatsFile) {
            def sentenceData = [
                    markovChain: markovAnalyzer.markovChain,
                    lengthStats: markovAnalyzer.lengthStats,
                    segmentStarts: markovAnalyzer.segmentStarts,
                    transitions: transitions
            ]
            new File(cfg.sentenceStatsFile).write(JsonOutput.prettyPrint(JsonOutput.toJson(sentenceData)))
            println "💾 Saved sentence stats to ${cfg.sentenceStatsFile}"
        }
    }
}

// ----------------------------------------------------------------
// --- GENERATOR ---
// ----------------------------------------------------------------

class Generator {
    WeightedSelector mainLengthSelector
    Map<String, WeightedSelector> typeSel = [:]
    Map<String, Map<String, WeightedSelector>> startSel = [:]
    Map<String, Map<String, WeightedSelector>> innerSel = [:]
    Map<String, Map<String, WeightedSelector>> lastSel = [:]
    Map<String, Map<String, WeightedSelector>> bigramStartSel = [:]
    Map<String, Map<String, WeightedSelector>> bigramInnerSel = [:]
    Map<String, Map<String, WeightedSelector>> bigramLastSel = [:]
    Map<String, Map<String, WeightedSelector>> trigramInnerSel = [:]
    Map<String, Map<String, WeightedSelector>> trigramLastSel = [:]
    Set<String> loadedVocabulary = null

    WeightedSelector transitionSel

    Map<String, WeightedSelector> segmentLengthSel = [:]
    Map<String, WeightedSelector> segmentStartSel = [:]
    Map<String, Map<String, WeightedSelector>> segmentMarkovSel = [:]

    void loadWordModel(Config cfg) {
        def data = new JsonSlurper().parse(new File(cfg.wordStatsFile))

        // Apply pruning if requested - remove all stats for words <= pruneMinTokens
        if (cfg.pruneMinTokens > 0) {
            println "✂️  Pruning model: removing stats for words with ≤ ${cfg.pruneMinTokens} tokens..."

            // Filter lengthStats (format: "5:C" -> count)
            def originalCount = data.lengthStats?.size() ?: 0
            data.lengthStats = data.lengthStats?.findAll { fusedKey, count ->
                def len = fusedKey.split(':')[0].toInteger()
                len > cfg.pruneMinTokens
            } ?: [:]

            // Filter all token distribution maps by effLen key
            def filterByEffLen = { Map dist ->
                dist?.findAll { effLen, inner ->
                    effLen.toInteger() > cfg.pruneMinTokens
                } ?: [:]
            }

            data.startTokens = filterByEffLen(data.startTokens)
            data.innerTokens = filterByEffLen(data.innerTokens)
            data.lastTokens = filterByEffLen(data.lastTokens)
            data.bigramStart = filterByEffLen(data.bigramStart)
            data.bigramInner = filterByEffLen(data.bigramInner)
            data.bigramLast = filterByEffLen(data.bigramLast)
            data.trigramInner = filterByEffLen(data.trigramInner)
            data.trigramLast = filterByEffLen(data.trigramLast)

            def remainingCount = data.lengthStats?.size() ?: 0
            println "   Length entries: ${originalCount} → ${remainingCount} (removed ${originalCount - remainingCount})"
        }

        mainLengthSelector = new WeightedSelector(data.lengthStats ?: [:])

        data.lengthStats?.each { fusedKey, count ->
            def startType = fusedKey.split(':')[1]
            typeSel[fusedKey] = new WeightedSelector([(startType): count])
        }

        data.startTokens?.each { effLen, byType ->
            byType.each { type, freqs ->
                startSel.computeIfAbsent(effLen, {[:]}).put(type, new WeightedSelector(freqs ?: [:]))
            }
        }
        data.innerTokens?.each { effLen, byType ->
            byType.each { type, freqs ->
                innerSel.computeIfAbsent(effLen, {[:]}).put(type, new WeightedSelector(freqs ?: [:]))
            }
        }
        data.lastTokens?.each { effLen, byType ->
            byType.each { type, freqs ->
                lastSel.computeIfAbsent(effLen, {[:]}).put(type, new WeightedSelector(freqs ?: [:]))
            }
        }
        data.bigramStart?.each { effLen, byPrev ->
            byPrev.each { prev, freqs ->
                bigramStartSel.computeIfAbsent(effLen, {[:]}).put(prev, new WeightedSelector(freqs ?: [:]))
            }
        }
        data.bigramInner?.each { effLen, byPrev ->
            byPrev.each { prev, freqs ->
                bigramInnerSel.computeIfAbsent(effLen, {[:]}).put(prev, new WeightedSelector(freqs ?: [:]))
            }
        }
        data.bigramLast?.each { effLen, byPrev ->
            byPrev.each { prev, freqs ->
                bigramLastSel.computeIfAbsent(effLen, {[:]}).put(prev, new WeightedSelector(freqs ?: [:]))
            }
        }
        data.trigramInner?.each { effLen, byPrev ->
            byPrev.each { prev, freqs ->
                trigramInnerSel.computeIfAbsent(effLen, {[:]}).put(prev, new WeightedSelector(freqs ?: [:]))
            }
        }
        data.trigramLast?.each { effLen, byPrev ->
            byPrev.each { prev, freqs ->
                trigramLastSel.computeIfAbsent(effLen, {[:]}).put(prev, new WeightedSelector(freqs ?: [:]))
            }
        }

        if (data.vocabulary) {
            loadedVocabulary = new HashSet<>(data.vocabulary)
            println "📚 Vocabulary loaded: ${loadedVocabulary.size()} words"
        }
    }

    void loadRhythmModel(String filepath) {
        def data = new JsonSlurper().parse(new File(filepath))

        data.lengthStats?.each { segType, counts ->
            Map<String, Integer> freqMap = [:]
            counts.eachWithIndex { val, idx -> freqMap["${val}"] = (freqMap["${val}"] ?: 0) + 1 }
            segmentLengthSel[segType] = new WeightedSelector(freqMap)
        }

        data.segmentStarts?.each { segType, starts ->
            Map<String, Integer> freqMap = [:]
            starts.each { s -> freqMap[s] = (freqMap[s] ?: 0) + 1 }
            segmentStartSel[segType] = new WeightedSelector(freqMap)
        }

        data.markovChain?.each { key, nexts ->
            segmentMarkovSel.computeIfAbsent(key, { new WeightedSelector(nexts) })
        }

        transitionSel = new WeightedSelector(data.transitions?.TERM ?: [:])
        println "✨ Loaded rhythm/structure model from ${filepath}"
    }

    Segment generateStructure(String segType, int depth) {
        if (depth > 10) return new Segment(type: segType, wordLengths: [], children: [])

        def targetLenSel = segmentLengthSel[segType]
        if (!targetLenSel) return new Segment(type: segType, wordLengths: [1, 2])

        def targetLen = targetLenSel.select(Globals.RND)?.toInteger() ?: 2

        def startStringSel = segmentStartSel[segType]
        def startString = startStringSel?.select(Globals.RND)
        def context = startString ? startString.split("\\|").toList() : []

        List<String> stream = []
        if (!context.isEmpty()) { stream.addAll(context) }
        else { stream << "WORD:MED" }

        while (stream.size() < targetLen) {
            def lookupKey = buildMarkovKey(segType, stream)
            def sel = segmentMarkovSel[lookupKey]
            if (!sel) break
            def next = sel.select(Globals.RND)
            if (!next || next == "END") break
            stream << next
        }

        Segment seg = new Segment(type: segType)

        stream.eachWithIndex { ent, i ->
            def parts = ent.split(':', 2)
            def eType = parts[0]
            def eVal = parts[1]

            if (eType == 'WORD') {
                int wordLen
                switch (eVal) {
                    case 'SHORT': wordLen = 1 + Globals.RND.nextInt(3); break
                    case 'MED': wordLen = 4 + Globals.RND.nextInt(4); break
                    case 'LONG': wordLen = 8 + Globals.RND.nextInt(5); break
                    default: wordLen = 3
                }
                seg.wordLengths << wordLen
            } else if (eType == 'PAUSE') {
                seg.children << new Segment(type: 'PAUSE', terminator: eVal, childInsertPosition: seg.wordLengths.size())
            } else if (eType == 'SEGMENT') {
                Segment child = generateStructure(eVal, depth + 1)
                child.childInsertPosition = seg.wordLengths.size()
                seg.children << child
            }
        }

        return seg
    }

    String buildMarkovKey(String segType, List<String> stream) {
        int maxDepth = 3
        def contextList = []
        for (int j = 1; j <= maxDepth; j++) {
            def idx = stream.size() - j
            if (idx >= 0) contextList.add(0, stream[idx])
            else contextList.add(0, "START")
        }
        return "${segType}|" + contextList.join("|")
    }

    String generateWord(int targetLen, int ngramMode) {
        def fused = mainLengthSelector.select(Globals.RND)
        String lenKey = targetLen.toString()
        WeightedSelector typeSel = this.typeSel[fused]
        if (!fused || !typeSel) {
            fused = "${targetLen}:C"
            typeSel = new WeightedSelector([C: 1])
        } else {
            if (!fused) return "blob"
            lenKey = fused.split(':')[0]; targetLen = lenKey.toInteger(); typeSel = new WeightedSelector([(fused.split(':')[1]): 1])
        }
        def startType = typeSel.select(Globals.RND)
        def effLen = Math.min(targetLen, Globals.MAX_EFF_LEN).toString()
        def tokens = []; tokens << (startSel[effLen]?[startType]?.select(Globals.RND) ?: "err"); def prevType = startType
        for (int i = 1; i < targetLen; i++) {
            def isLast = (i == targetLen - 1); def token = null
            if (ngramMode >= 3 && tokens.size() >= 2) {
                def tgKey = "${tokens[-2]}:${tokens[-1]}"; token = (isLast ? trigramLastSel : trigramInnerSel)[effLen]?[tgKey]?.select(Globals.RND)
            }
            if (!token && ngramMode >= 2) {
                def bgKey = tokens[-1]; token = (isLast ? bigramLastSel : (i == 1 ? bigramStartSel : bigramInnerSel))[effLen]?[bgKey]?.select(Globals.RND)
            }
            if (!token) token = (isLast ? lastSel : innerSel)[effLen]?[prevType]?.select(Globals.RND)
            if (!token) token = (prevType == 'C') ? 'a' : 'b'
            tokens << token; prevType = (prevType == 'C' ? 'V' : 'C')
        }
        return tokens.join('')
    }

    String formatMarkedWord(String w, Config cfg, int startMode, int actualMode, boolean forced) {
        String result = w

        // Apply bracketing if enabled (shows fallbacks only)
        if (cfg.bracketMode) {
            if (forced) {
                result = "[${result}]"  // Vocabulary word
            } else if (actualMode < startMode) {
                // Only bracket if we fell back to a lower n-gram level
                if (actualMode == 2) result = "<${result}>"  // Fell back to bigram
                else if (actualMode == 1) result = "{${result}}"  // Fell back to unigram
            }
        }

        // Apply coloring if enabled (shows generation method for ALL words)
        if (cfg.colorMode) {
            def BLUE = "\u001B[34m"      // Trigram
            def GREEN = "\u001B[32m"     // Bigram
            def ORANGE = "\u001B[38;5;208m"  // Unigram
            def RED = "\u001B[31m"       // Vocabulary word
            def RESET = "\u001B[0m"

            if (forced) result = "${RED}${result}${RESET}"
            else if (actualMode == 3) result = "${BLUE}${result}${RESET}"
            else if (actualMode == 2) result = "${GREEN}${result}${RESET}"
            else if (actualMode == 1) result = "${ORANGE}${result}${RESET}"
        }

        return result
    }

    String generateAndProcessWord(int len, Config cfg, Map stats) {
        int startMode = cfg.ngramMode
        int currentMode = startMode
        String w = generateWord(len, currentMode)
        boolean forced = false

        if (cfg.vocabFilter && loadedVocabulary) {
            int retries = 0
            while (loadedVocabulary.contains(w)) {
                stats.filteredCount++
                retries++
                if (retries >= 20) { if (startMode > 2) currentMode = 2 }
                if (retries >= 40) { if (startMode > 1) currentMode = 1 }
                if (retries >= 60) { stats.forcedCount++; forced = true; break }
                w = generateWord(len, currentMode)
            }
        }

        // Apply marking modes if enabled
        if (cfg.bracketMode || cfg.colorMode) {
            w = formatMarkedWord(w, cfg, startMode, currentMode, forced)
        }

        // Apply uppercase mode if enabled (only for word list mode)
        if (cfg.uppercaseMode && !cfg.sentenceStatsFile && w.length() > 0) {
            // Strip ANSI codes if present, capitalize, then re-add
            def ansiPattern = ~/\u001B\[[0-9;]+m/
            def matches = (w =~ ansiPattern)
            if (matches.size() > 0) {
                // Has ANSI codes - preserve them
                def prefix = matches[0]
                def rest = w.substring(prefix.length())
                if (rest.length() > 0) {
                    rest = rest[0].toUpperCase() + (rest.length() > 1 ? rest.substring(1) : "")
                }
                w = prefix + rest
            } else {
                // No ANSI codes - simple capitalize
                w = w[0].toUpperCase() + (w.length() > 1 ? w.substring(1) : "")
            }
        }

        return w
    }

    String renderSegment(Segment seg, Config cfg, Map stats) {
        def sb = new StringBuilder()

        // Add opening punctuation normally - it will be uncolored
        if (seg.type == 'QUOTE') sb.append('"')
        else if (seg.type == 'SQUOTE') sb.append("'")
        else if (seg.type == 'PAREN') sb.append("(")
        else if (seg.type == 'DASH') sb.append(" \u2014 ")  // Space before and after opening dash

        def childIdx = 0
        def sortedChildren = seg.children.sort { it.childInsertPosition }

        seg.wordLengths.eachWithIndex { len, i ->
            while (childIdx < sortedChildren.size() && sortedChildren[childIdx].childInsertPosition == i) {
                def child = sortedChildren[childIdx]
                if (child.type == 'PAUSE') {
                    if (sb.length() > 0 && sb.charAt(sb.length() - 1) == ' ') sb.setLength(sb.length() - 1)
                    sb.append(child.terminator).append(' ')
                } else if (child.type == 'HYPHEN') {
                    if (sb.length() > 0 && sb.charAt(sb.length() - 1) == ' ') sb.setLength(sb.length() - 1)
                    sb.append('-')
                } else {
                    def out = renderSegment(child, cfg, stats)
                    if (out) sb.append(out).append(' ')
                }
                childIdx++
            }

            String w = generateAndProcessWord(len, cfg, stats)
            if (i == 0 && (seg.type == 'SENTENCE' || seg.type == 'QUOTE')) w = Utils.smartCaps(w)

            boolean isLastWord = (i == seg.wordLengths.size() - 1)

            // For the last word in QUOTE/SQUOTE/PAREN, handle closing punctuation
            if (isLastWord && (seg.type == 'QUOTE' || seg.type == 'SQUOTE' || seg.type == 'PAREN')) {
                // If word has ANSI codes, insert closing punctuation AFTER the reset code
                if (cfg.colorMode && w.contains("\u001B[0m")) {
                    def resetIdx = w.lastIndexOf("\u001B[0m")
                    def closePunct = seg.type == 'QUOTE' ? '"' : seg.type == 'SQUOTE' ? "'" : ")"
                    // Append: colored word + reset code + uncolored punctuation
                    sb.append(w.substring(0, resetIdx + 4))  // Include the \u001B[0m (4 chars)
                    sb.append(closePunct)                     // Closing punctuation (uncolored)
                    sb.append(w.substring(resetIdx + 4))     // Any remaining content
                    // Add terminator if present
                    if (seg.terminator) sb.append(seg.terminator)
                    // Don't add space or process closing punctuation again later
                    seg.type = 'DONE'  // Mark as handled
                } else {
                    sb.append(w).append(' ')
                }
            } else {
                sb.append(w).append(' ')
            }
        }

        while (childIdx < sortedChildren.size()) {
            def child = sortedChildren[childIdx]
            if (child.type != 'PAUSE' && child.type != 'HYPHEN') {
                def out = renderSegment(child, cfg, stats)
                if (out) sb.append(out).append(' ')
            }
            childIdx++
        }

        if (sb.length() > 0 && sb.charAt(sb.length() - 1) == ' ') sb.setLength(sb.length() - 1)

        // Only add closing punctuation if not already handled inline
        if (seg.type == 'QUOTE') {
            if (seg.terminator) sb.append(seg.terminator)
            sb.append('"')
        }
        else if (seg.type == 'SQUOTE') {
            sb.append("'")
            if (seg.terminator) sb.append(seg.terminator)
        }
        else if (seg.type == 'PAREN') {
            sb.append(")")
            if (seg.terminator) sb.append(seg.terminator)
        }
        else if (seg.type == 'DASH') {
            // Only add closing dash if there's no terminator (paired aside dash)
            // If there's a terminator, it means we're at sentence end - don't add closing dash
            if (!seg.terminator) {
                sb.append(" \u2014")  // Space before closing dash
            } else {
                // Add the terminator directly (it's really a sentence terminator, not dash-specific)
                sb.append(seg.terminator)
            }
        }
        else if (seg.type == 'SENTENCE') {
            if (seg.terminator) sb.append(seg.terminator)
        }
        // 'DONE' type means closing punctuation was already handled inline - skip it

        return sb.toString()
    }

    void execute(Config cfg) {
        if (cfg.vocabFilter && !loadedVocabulary) { println "❌ Error: Vocab Filter requires vocabulary."; System.exit(1) }
        if (cfg.vocabFilter) println "✨ Vocab Filter: ${loadedVocabulary.size()} words loaded."

        PrintStream outStream = System.out
        if (cfg.f_File) { outStream = new PrintStream(new File(cfg.f_File)); println "✍️  Writing to file: ${cfg.f_File}" }
        else { println "\n✍️  OUTPUT:"; println "=" * 60 }

        int generatedCount = 0
        def stats = [filteredCount: 0, forcedCount: 0]

        while (generatedCount < cfg.itemCount) {
            if (cfg.sentenceStatsFile) {
                Segment root = generateStructure("SENTENCE", 0)
                root.terminator = transitionSel?.select(Globals.RND) ?: "."

                String output = renderSegment(root, cfg, stats)
                output = Utils.smartCaps(output)

                if (cfg.lineOutput) outStream.println(output)
                else outStream.print(output + " ")

                generatedCount++
            } else {
                def fused = mainLengthSelector.select(Globals.RND)
                if (!fused) break
                def targetLen = fused.split(':')[0].toInteger()

                String w = generateAndProcessWord(targetLen, cfg, stats)

                if (cfg.lineOutput) outStream.println(w)
                else outStream.print(w + " ")

                generatedCount++
            }
        }
        if (cfg.f_File) outStream.close()
        println ""
        println "=" * 60
        if (cfg.vocabFilter) {
            println "🔒 Filtered ${stats.filteredCount} duplicates."
            if (stats.forcedCount > 0) println "⚠️  Forced to accept ${stats.forcedCount} real words."
        }
        println "✅ Done."
    }
}

// ----------------------------------------------------------------
// --- USAGE / HELP ---
// ----------------------------------------------------------------

def printUsage() {
    println """
╔════════════════════════════════════════════════════════════════════════════╗
║                   Text Analyzer & Mimicker Tool                            ║
║             Statistical text analysis and generation tool                  ║
╚════════════════════════════════════════════════════════════════════════════╝

USAGE:
  Analysis Mode:
    groovy TextAnalyzerMimicker.groovy -a -f <input.txt> -w <words.json> [OPTIONS]
  
  Generation Mode:
    groovy TextAnalyzerMimicker.groovy -g -w <words.json> [OPTIONS]

MODES:
  -a                  Analysis mode - analyze input text and build statistical model
  -g                  Generation mode - generate text from statistical model
  -h, --help          Display this help message

ANALYSIS MODE OPTIONS:
  -f <file>           Input text file to analyze (required)
  -w <file>           Output word statistics JSON file (required)
  -s <file>           Output sentence/rhythm statistics JSON file (optional)
  -v                  Vocab filter mode - save vocabulary list for duplicate filtering
  -q {1|2}            Quote handling mode (default: 2)
                        1 = Strict validation (parser-level, rejects invalid quotes)
                        2 = Simple tokenizer (uppercase heuristic for contractions)
  -d                  Debug mode - print parse tree during analysis

GENERATION MODE OPTIONS:
  -w <file>           Input word statistics JSON file (required)
  -s <file>           Input sentence/rhythm statistics JSON file (optional)
                      If omitted, generates word list only
  -f <file>           Output file (optional, default: STDOUT)
  -n <count>          Number of items to generate (default: 20)
  -l                  Line-by-line output format (default: space-separated)
  
  N-Gram Mode:
    (default)           Unigram mode (character frequency only)
    -b                  Bigram mode (2-character sequences)
    -t                  Trigram mode (3-character sequences)
  
  Filtering Options:
    -v                Vocab filter - avoid duplicating vocabulary from analysis
                      (requires -v flag was used during analysis)
    -p <n>            Prune mode - exclude words with ≤ n tokens from model
                      Example: -p 3 generates only words with 4+ tokens
  
  Display Options:
    -u                Uppercase - capitalize first character of each word
                      (only valid in word list mode, not with -s)
    -m                Fallback brackets - mark n-gram fallbacks with brackets
                      <word> = fell back to bigram from trigram target
                      {word} = fell back to unigram
                      [word] = forced to use vocabulary word (duplicate)
    -c                N-gram coloring - color words by generation method
                      Blue   = trigram (3-char sequences)
                      Green  = bigram (2-char sequences)
                      Orange = unigram (1-char frequency)
                      Red    = vocabulary word (duplicate)
    
    NOTE: -m and -c can be combined for both brackets and colors

EXAMPLES:
  Analyze a text file with vocabulary saving:
    groovy TextAnalyzerMimicker.groovy -a -f input.txt -w words.json -s sentences.json -v

  Generate 50 unique words (4+ tokens), capitalized, using trigram mode:
    groovy TextAnalyzerMimicker.groovy -g -w words.json -v -n 50 -l -p 3 -u -t

  Generate with color-coded n-gram levels (unigram default):
    groovy TextAnalyzerMimicker.groovy -g -w words.json -n 30 -c

  Generate with both brackets (fallbacks) and colors (all words), trigram mode:
    groovy TextAnalyzerMimicker.groovy -g -w words.json -n 30 -m -c -t

  Generate sentences with fallback brackets, bigram mode:
    groovy TextAnalyzerMimicker.groovy -g -w words.json -s sentences.json -b -n 10 -m

  Generate to file with all features:
    groovy TextAnalyzerMimicker.groovy -g -w words.json -f output.txt -v -u -n 100 -p 4 -c

NOTES:
  - Analysis creates statistical models from input text
  - Word model captures character patterns and token structure
  - Sentence model (optional) captures rhythm, punctuation, and structure
  - Vocab filter (-v) requires vocabulary saved during analysis
  - Prune mode filters the statistical model at load time for efficiency
  - Fallback brackets (-m) show only when generator had to fall back
  - N-gram coloring (-c) shows how ALL words were generated, regardless of target
  - Both marking modes can be combined for maximum visibility
  - Uppercase mode only works in word list mode (no -s sentence file)
"""
}

// ----------------------------------------------------------------
// --- MAIN ---
// ----------------------------------------------------------------

def cfg = parseArgs(args)

if (!cfg.valid) {
    if (cfg.error != "Help requested") {
        println "❌ Error: ${cfg.error}\n"
    }
    printUsage()
    System.exit(1)
}
println cfg.toString()

if (cfg.analyze) {
    def analyzer = new Analyzer()
    analyzer.process(cfg)
    analyzer.save(cfg)
}
if (cfg.generate) {
    def generator = new Generator()
    generator.loadWordModel(cfg)
    if (cfg.sentenceStatsFile) generator.loadRhythmModel(cfg.sentenceStatsFile)
    generator.execute(cfg)
}