package org.tdl.ptg.split

import groovy.transform.Canonical
import groovy.json.JsonSlurper
import groovy.json.JsonOutput
import java.nio.file.Files
import java.nio.file.Paths
import java.util.regex.Pattern

// ----------------------------------------------------------------
// --- SHARED GLOBALS & UTILS ---
// ----------------------------------------------------------------

class Globals {
    static final Random RND = new Random()
    static final Set<String> VOWELS = new HashSet(['a', 'e', 'i', 'o', 'u', 'y', 'æ', 'ø', 'å', 'ä', 'ö', 'ü', 'é'])
    // \u00B6 = Pilcrow (Specific Newline Terminator for Attributions)
    static final Set<String> TERMINATORS = new HashSet(['.', '!', '?', '\u2026', '\u00B6'])
    static final Set<String> PAUSES = new HashSet([',', ';', ':'])
    // \uE000 = Internal Marker for Valid Attribution Dashes
    static final Set<String> ALL_PUNCT = new HashSet(['.', '!', '?', ',', ':', ';',
                                                      '\u2014', '-', '\u2026', '\u00B6', '\uE000',
                                                      '\u0022', '\u201C', '\u201D',
                                                      '\u0027', '\u2018', '\u2019',
                                                      '(', ')', '[', ']', '{', '}'])
    static final int MAX_EFF_LEN = 8
}

// ----------------------------------------------------------------
// --- PUNCTUATION & TOKEN HANDLING ---
// ----------------------------------------------------------------

@Canonical
class PunctToken {
    String type
    String value
    String context
}

@Canonical
class Token {
    String type = 'WORD'
    String text = ''
    String value = ''
    String context = ''
    int wordLength = 0
}

class Punctuation {
    static boolean isApostrophe(String fullText, int pos) {
        if (pos <= 0 || pos >= fullText.length() - 1) return false
        def before = fullText[pos - 1]
        def after = fullText[pos + 1]
        if (before =~ /\w/ && after =~ /\w/) return true
        if (pos < fullText.length() - 1 && fullText[pos + 1] =~ /\d/) return true
        return false
    }

    static PunctToken classifySingle(String s) {
        switch(s) {
            case '.': return new PunctToken('TERM', s, 'SENTENCE')
            case '!': return new PunctToken('TERM', s, 'SENTENCE')
            case '?': return new PunctToken('TERM', s, 'SENTENCE')
            case '\u2026': return new PunctToken('TERM', s, 'ELLIPSIS')
            case '\u00B6': return new PunctToken('TERM', s, 'NEWLINE')

            case ',': return new PunctToken('PAUSE', s, 'CLAUSE')
            case ';': return new PunctToken('PAUSE', s, 'CLAUSE')
            case ':': return new PunctToken('PAUSE', s, 'INTRO')

            case '\u2014': return new PunctToken('LINK', s, 'DASH')
            case '-': return new PunctToken('LINK', s, 'HYPHEN')

                // STRICT ATTRIBUTION DASH MARKER
            case '\uE000': return new PunctToken('LINK', '\u2014', 'ATTR_MARKER')

            case '(': return new PunctToken('OPEN', s, 'PAREN')
            case ')': return new PunctToken('CLOSE', s, 'PAREN')
            case '[': return new PunctToken('OPEN', s, 'BRACKET')
            case ']': return new PunctToken('CLOSE', s, 'BRACKET')
            case '{': return new PunctToken('OPEN', s, 'BRACE')
            case '}': return new PunctToken('CLOSE', s, 'BRACE')

            case '\u201C': return new PunctToken('OPEN', s, 'QUOTE')
            case '\u201D': return new PunctToken('CLOSE', s, 'QUOTE')
            case '\u2018': return new PunctToken('OPEN', s, 'SQUOTE')
            case '\u2019': return new PunctToken('CLOSE', s, 'SQUOTE')

            case '\u0022': return new PunctToken('UNKNOWN', s, 'QUOTE')
            case '\u0027': return new PunctToken('UNKNOWN', s, 'SQUOTE')

            default: return null
        }
    }

    static String guessOpenClose(String prevText, String nextText) {
        def prevEndsWord = prevText && prevText =~ /[\w.,!?]$/
        // Include our special markers in the spacing logic
        def prevEndsSpace = !prevText || prevText =~ /[\s(\[{—\u00B6\uE000]$/
        def nextStartsWord = nextText && nextText =~ /^\w/
        def nextStartsSpace = !nextText || nextText =~ /^[\s)\]}.,:;!?—\u00B6\uE000]/

        if (prevEndsSpace && nextStartsWord) return 'OPEN'
        if (prevEndsWord && nextStartsSpace) return 'CLOSE'
        return 'UNKNOWN'
    }
}

class Utils {
    static List tokenizeStructure(String word) {
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

    static String normalizeQuotes(String text) {
        def result = new StringBuilder()
        def chars = text.toCharArray()
        def STRAIGHT_DOUBLE = '\u0022' as char
        def STRAIGHT_SINGLE = '\u0027' as char
        def OPEN_DOUBLE = '\u201C'
        def CLOSE_DOUBLE = '\u201D'
        def OPEN_SINGLE = '\u2018'
        def CLOSE_SINGLE = '\u2019'

        for (int i = 0; i < chars.length; i++) {
            def c = chars[i]
            if (c == STRAIGHT_DOUBLE) {
                def prev = i > 0 ? chars[i-1].toString() : ''
                def next = i < chars.length - 1 ? chars[i+1].toString() : ''
                def guess = Punctuation.guessOpenClose(prev, next)
                result.append(guess == 'OPEN' ? OPEN_DOUBLE : CLOSE_DOUBLE)
            } else if (c == STRAIGHT_SINGLE) {
                if (Punctuation.isApostrophe(text, i)) {
                    result.append(CLOSE_SINGLE)
                } else {
                    def prev = i > 0 ? chars[i-1].toString() : ''
                    def next = i < chars.length - 1 ? chars[i+1].toString() : ''
                    def guess = Punctuation.guessOpenClose(prev, next)
                    result.append(guess == 'OPEN' ? OPEN_SINGLE : CLOSE_SINGLE)
                }
            } else {
                result.append(c)
            }
        }
        return result.toString()
    }

    static String normalizeEllipsis(String text) {
        return text.replaceAll(/\.{3,}/, '\u2026').replaceAll(/\s*\u2026\s*/, '\u2026 ')
    }

    static String normalizeDashes(String text) {
        return text.replaceAll(/--+/, '\u2014').replaceAll(/\s*\u2014\s*/, ' \u2014 ')
    }

    // Recursive Capitalization for Smart Outputs (handles quotes/brackets)
    static String smartCaps(String s) {
        if (!s) return s
        // Find first actual letter index
        def m = (s =~ /[a-zA-Z]/)
        if (m.find()) {
            int idx = m.start()
            // Capitalize that char
            return s.substring(0, idx) + s.substring(idx, idx+1).toUpperCase() + s.substring(idx+1)
        }
        return s
    }
}

// ----------------------------------------------------------------
// --- HIERARCHICAL SEGMENT MODEL ---
// ----------------------------------------------------------------

@Canonical
class Segment {
    String type = 'SENTENCE'
    List<Integer> wordLengths = []
    List<Segment> children = []
    int childInsertPosition = -1
    String openPunct = ''
    String closePunct = ''
    String terminator = ''
}

// ----------------------------------------------------------------
// --- STRUCTURE TOKENIZER & PARSER ---
// ----------------------------------------------------------------

class StructureTokenizer {
    List<Token> tokenize(String text) {
        def normalized = Utils.normalizeEllipsis(Utils.normalizeDashes(Utils.normalizeQuotes(text)))
        def tokens = []
        def words = normalized.split(/\s+/)
        words.each { word -> if (word) tokens.addAll(tokenizeWord(word)) }
        return tokens
    }

    List<Token> tokenizeWord(String word) {
        def tokens = []
        def current = new StringBuilder()
        def chars = word.toCharArray()
        int i = 0
        while (i < chars.length) {
            def c = chars[i].toString()
            if (c == '\u2026' || c == '\u00B6' || c == '\uE000') {
                if (current.length() > 0) { tokens << createWordToken(current.toString()); current = new StringBuilder() }
                tokens << createPunctToken(Punctuation.classifySingle(c)); i++; continue
            }
            def punct = Punctuation.classifySingle(c)
            if (punct) {
                if (current.length() > 0) { tokens << createWordToken(current.toString()); current = new StringBuilder() }
                tokens << createPunctToken(punct)
            } else {
                current.append(c)
            }
            i++
        }
        if (current.length() > 0) tokens << createWordToken(current.toString())
        return tokens
    }

    Token createWordToken(String text) {
        def struct = Utils.tokenizeStructure(text)
        def hasVowel = text.toLowerCase().any { Globals.VOWELS.contains(it.toString()) }
        if (!struct || !hasVowel) return null
        return new Token(type: 'WORD', text: text, wordLength: struct.size())
    }

    Token createPunctToken(PunctToken p) {
        return p ? new Token(type: p.type, value: p.value, context: p.context) : null
    }
}

class StructureBalancer {
    List<Token> balance(List<Token> tokens) {
        resolveUnknownQuotes(tokens)
        balanceStructures(tokens)
        return tokens.findAll { it.type != 'DISCARD' }
    }

    void resolveUnknownQuotes(List<Token> tokens) {
        tokens.eachWithIndex { tok, i ->
            if (tok.type == 'UNKNOWN') {
                def prevText = '', nextText = ''
                for (int j = i - 1; j >= 0; j--) { if (tokens[j].type == 'WORD' || tokens[j].value) { prevText = tokens[j].text ?: tokens[j].value; break } }
                for (int j = i + 1; j < tokens.size(); j++) { if (tokens[j].type == 'WORD' || tokens[j].value) { nextText = tokens[j].text ?: tokens[j].value; break } }
                tok.type = (Punctuation.guessOpenClose(prevText, nextText) == 'OPEN') ? 'OPEN' : 'CLOSE'
            }
        }
    }

    void balanceStructures(List<Token> tokens) {
        def sentenceStart = 0
        for (int i = 0; i <= tokens.size(); i++) {
            if (i < tokens.size() && tokens[i].type == 'TERM') {
                int sentenceEnd = i + 1
                while (sentenceEnd < tokens.size() && tokens[sentenceEnd].type == 'CLOSE') sentenceEnd++
                if (sentenceEnd > sentenceStart) balanceSentence(tokens, sentenceStart, sentenceEnd)
                sentenceStart = sentenceEnd
            } else if (i == tokens.size() && i > sentenceStart) {
                balanceSentence(tokens, sentenceStart, i)
            }
        }
    }

    void balanceSentence(List<Token> tokens, int start, int end) {
        ['QUOTE', 'SQUOTE', 'PAREN', 'BRACKET', 'BRACE'].each { context ->
            def stack = []
            for (int i = start; i < end; i++) {
                def tok = tokens[i]
                if (tok.type == 'DISCARD') continue
                if (tok.type == 'OPEN' && tok.context == context) stack.push(i)
                else if (tok.type == 'CLOSE' && tok.context == context) {
                    if (stack) stack.pop()
                    else tok.type = 'DISCARD'
                }
            }
            stack.reverse().each { openIdx -> tokens[openIdx].type = 'DISCARD' }
        }
    }
}

class StructureAnalyzer {
    List<Segment> parseToSegments(List<Token> tokens) {
        def segments = []
        def stack = [new Segment(type: 'SENTENCE')]
        def lastWasPause = false

        tokens.each { tok ->
            if (stack.isEmpty()) stack = [new Segment(type: 'SENTENCE')]
            def current = stack.first()

            switch (tok.type) {
                case 'WORD':
                    if (tok.wordLength > 0) {
                        current.wordLengths << tok.wordLength
                        lastWasPause = false
                    }
                    break

                case 'OPEN':
                    def child = new Segment(type: tok.context, openPunct: tok.value, childInsertPosition: current.wordLengths.size())
                    current.children << child
                    stack.push(child)
                    lastWasPause = false
                    break

                case 'CLOSE':
                    if (stack.size() > 1 && stack.first().type == tok.context) {
                        stack.first().closePunct = tok.value
                        stack.pop()
                    }
                    lastWasPause = false
                    break

                case 'TERM':
                    if (stack.size() > 1 && stack.first().type in ['QUOTE', 'SQUOTE']) {
                        stack.first().terminator = tok.value
                    } else {
                        while (stack.size() > 1) stack.pop()
                        current = stack.first()
                        current.terminator = tok.value
                        if (current.wordLengths || current.children) segments << current
                        stack = [new Segment(type: 'SENTENCE')]
                    }
                    lastWasPause = false
                    break

                case 'PAUSE':
                    if (current.wordLengths && !lastWasPause) {
                        current.children << new Segment(type: 'PAUSE', terminator: tok.value, childInsertPosition: current.wordLengths.size())
                        lastWasPause = true
                    }
                    break

                case 'LINK':
                    if (tok.context == 'ATTR_MARKER') {
                        // EXPLICIT ATTRIBUTION
                        def attr = new Segment(
                                type: 'ATTRIBUTION',
                                openPunct: tok.value,
                                childInsertPosition: current.wordLengths.size()
                        )
                        current.children << attr
                        stack.push(attr)
                    } else {
                        // Standard mid-sentence dash (preserved if valid)
                        def aside = new Segment(type: 'DASH_ASIDE', openPunct: tok.value, childInsertPosition: current.wordLengths.size())
                        current.children << aside
                        stack.push(aside)
                    }
                    lastWasPause = false
                    break
            }
        }
        return segments
    }
}

// ----------------------------------------------------------------
// --- CONFIGURATION ---
// ----------------------------------------------------------------

@Canonical
class Config {
    boolean analyze = false
    boolean generate = false
    String wordStatsFile = null
    String sentenceStatsFile = null
    String f_File = null
    boolean uniqueMode = false
    int pruneMinTokens = 0
    int ngramMode = 3
    int count = 20
    boolean valid = false
    String error = ""

    String toString() {
        if (analyze) {
            return """
=== ANALYSIS CONFIGURATION ===
Input File        : ${f_File}
Word Stats Out    : ${wordStatsFile}
Sentence Stats Out: ${sentenceStatsFile ?: '(None)'}
Unique Mode       : ${uniqueMode ? 'Active (Saving vocabulary list)' : 'Inactive'}
=============================="""
        } else {
            return """
=== GENERATION CONFIGURATION ===
Word Stats In     : ${wordStatsFile}
Sentence Stats In : ${sentenceStatsFile ?: '(None) - Word List Mode'}
Output File       : ${f_File ?: 'STDOUT'}
Pruning           : ${pruneMinTokens > 0 ? "Min ${pruneMinTokens} tokens" : 'Disabled'}
N-Gram Mode       : ${ngramMode} (Initial Target)
Unique Mode       : ${uniqueMode ? 'Active (Fallback: Tri->Bi->Uni)' : 'Inactive'}
Count             : ${count}
================================"""
        }
    }
}

def parseArgs(String[] args) {
    def cfg = new Config()
    def i = 0
    while (i < args.length) {
        def arg = args[i]
        switch (arg) {
            case '-a': cfg.analyze = true; break
            case '-g': cfg.generate = true; break
            case '-w': cfg.wordStatsFile = args[++i]; break
            case '-s': cfg.sentenceStatsFile = args[++i]; break
            case '-f': cfg.f_File = args[++i]; break
            case '-u': cfg.uniqueMode = true; break
            case '-p': cfg.pruneMinTokens = args[++i].toInteger(); break
            case '-c': cfg.count = args[++i].toInteger(); break
            case '-1': cfg.ngramMode = 1; break
            case '-2': case '-b': cfg.ngramMode = 2; break
            case '-3': case '-t': cfg.ngramMode = 3; break
        }
        i++
    }
    if (cfg.analyze && cfg.generate) { cfg.error = "Ambiguous Mode: Select either -a or -g."; return cfg }
    if (!cfg.analyze && !cfg.generate) { cfg.error = "No Mode Specified: Use -a or -g."; return cfg }
    if (cfg.analyze) {
        if (!cfg.f_File) { cfg.error = "Analysis requires Source Text (-f)."; return cfg }
        if (!cfg.wordStatsFile) { cfg.error = "Analysis requires Output File (-w)."; return cfg }
    }
    if (cfg.generate) {
        if (!cfg.wordStatsFile) { cfg.error = "Generation requires Input Stats (-w)."; return cfg }
    }
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
// --- STRUCTURAL MARKOV LOGIC ---
// ----------------------------------------------------------------

class StructuralMarkovAnalyzer {
    Map<String, Map<String, Integer>> markovChain = [:]
    Map<String, List<Integer>> lengthStats = [:]
    Map<String, List<String>> segmentStarts = [:]
    Map<String, Map<String, Integer>> openPunctStats = [:]
    Map<String, Map<String, Integer>> closePunctStats = [:]

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
        if (seg.openPunct) openPunctStats.computeIfAbsent(seg.type, {[:]}).merge(seg.openPunct, 1, Integer::sum)
        if (seg.closePunct) closePunctStats.computeIfAbsent(seg.type, {[:]}).merge(seg.closePunct, 1, Integer::sum)

        int entityCount = seg.wordLengths.size() + seg.children.size()
        lengthStats.computeIfAbsent(seg.type, {[]}).add(entityCount)

        Map<Integer, String> streamMap = [:]
        seg.wordLengths.eachWithIndex { len, i -> streamMap[i] = encodeEntity("WORD", binWordLen(len)) }
        seg.children.each { child ->
            processSegment(child)
            String val = (child.type == 'PAUSE') ? (child.terminator ?: ',') : child.type
            streamMap[child.childInsertPosition] = encodeEntity((child.type == 'PAUSE' ? 'PAUSE' : 'SEGMENT'), val)
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
    StructureTokenizer tokenizer = new StructureTokenizer()
    StructureBalancer balancer = new StructureBalancer()
    StructureAnalyzer structureAnalyzer = new StructureAnalyzer()
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

    void process(Config cfg) {
        println "📖 Analyzing source..."
        def fileContent = new File(cfg.f_File).getText("UTF-8")
        if (fileContent.length() > 0 && fileContent.charAt(0) == '\uFEFF') fileContent = fileContent.substring(1)

        def text = fileContent
        text = text.replaceAll(/--+/, '\u2014') // Normalize all dashes to em-dash

        // 1. IDENTIFY ATTRIBUTIONS (Quote + Space* + Dash + Space* + Words + EndOfLine)
        // Replaces the Dash with \uE000 and the EndOfLine with \u00B6
        // This PROTECTS valid attributions from pruning.
        text = text.replaceAll(/([”"])\s*\u2014\s*([^\s\n\r].*?)(\r?\n|$)/, '$1 \uE000 $2 \u00B6 ')

        // 2. PRUNE INVALID DASHES
        // "Any other dash preceded by whitespace and followed by non-whitespace is removed entirely"
        // Pattern: [Space] [Dash] [Non-Space]
        // Replacement: [Space] (keeps words separated but removes the dash)
        text = text.replaceAll(/(\s)\u2014(?=\S)/, '$1')

        // 3. PARAGRAPH & NEWLINE HANDLING
        // Double Newline = Paragraph Break (Sentence Terminator)
        text = text.replaceAll(/\r?\n\r?\n/, " . ")
        // Single Newline = Space (Paragraph Wrap), UNLESS it matches the \u00B6 we just added
        text = text.replaceAll(/\r?\n/, " ")

        def tokens = tokenizer.tokenize(text)
        tokens = balancer.balance(tokens.findAll { it != null })

        int wordsProcessed = 0
        tokens.findAll { it.type == 'WORD' && it.wordLength > 0 }.each { tok ->
            def clean = tok.text.findAll { Character.isLetter(it as char) }.join('').toLowerCase()
            def struct = Utils.tokenizeStructure(tok.text)
            if (struct) {
                wordsProcessed++
                if (cfg.uniqueMode) vocabulary.add(clean)
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
                            def prev2 = struct[idx-2][1]
                            def tgKey = "${prev2}:${prevContent}"
                            def tgMap = (idx == len - 1) ? trigramLast : trigramInner
                            Utils.ensureMap(tgMap, effLen, tgKey)[content] = (Utils.ensureMap(tgMap, effLen, tgKey)[content] ?: 0) + 1
                        }
                    }
                }
            }
        }

        if (cfg.sentenceStatsFile) {
            def segments = structureAnalyzer.parseToSegments(tokens)
            markovAnalyzer.train(segments)
            segments.each { s ->
                if (s.terminator) transitions.computeIfAbsent('TERM', {[:]}).merge(s.terminator, 1, Integer::sum)
            }
        }

        println "\n📊 Analysis Stats:"
        println "   Words Processed: ${wordsProcessed}"
        if (cfg.uniqueMode) println "   Unique Vocabulary: ${vocabulary.size()} words stored"
        if (cfg.sentenceStatsFile) {
            println "   Structural Rules: ${markovAnalyzer.markovChain.size()} contexts learned"
            println "   Structure Types: ${markovAnalyzer.lengthStats.keySet()}"
        }
        println ""
    }

    void save(Config cfg) {
        def wordData = [
                vocabulary: (cfg.uniqueMode ? vocabulary : null),
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
        Files.write(Paths.get(cfg.wordStatsFile), JsonOutput.toJson(wordData).getBytes("UTF-8"))
        println "💾 Word Stats -> ${cfg.wordStatsFile}"

        if (cfg.sentenceStatsFile) {
            def rhythmData = [
                    markovChain: markovAnalyzer.markovChain,
                    lengthStats: markovAnalyzer.lengthStats,
                    segmentStarts: markovAnalyzer.segmentStarts,
                    openPunctStats: markovAnalyzer.openPunctStats,
                    closePunctStats: markovAnalyzer.closePunctStats,
                    transitions: transitions
            ]
            Files.write(Paths.get(cfg.sentenceStatsFile), JsonOutput.toJson(rhythmData).getBytes("UTF-8"))
            println "💾 Sentence Stats -> ${cfg.sentenceStatsFile}"
        }
    }
}

// ----------------------------------------------------------------
// --- GENERATOR ---
// ----------------------------------------------------------------

class Generator {
    WeightedSelector mainLengthSelector
    Map<String, WeightedSelector> lengthToStartTypeSel = [:]
    Map<String, Map<String, WeightedSelector>> startSel = [:]
    Map<String, Map<String, WeightedSelector>> innerSel = [:]
    Map<String, Map<String, WeightedSelector>> lastSel = [:]
    Map<String, Map<String, WeightedSelector>> bigramStartSel = [:]
    Map<String, Map<String, WeightedSelector>> bigramInnerSel = [:]
    Map<String, Map<String, WeightedSelector>> bigramLastSel = [:]
    Map<String, Map<String, WeightedSelector>> trigramInnerSel = [:]
    Map<String, Map<String, WeightedSelector>> trigramLastSel = [:]

    Set<String> loadedVocabulary = null
    Map<String, WeightedSelector> transitionSel = [:]

    // Markov Data
    Map<String, Map<String, WeightedSelector>> markovSelectors = [:]
    Map<String, List<Integer>> lengthStats = [:]
    Map<String, List<String>> segmentStarts = [:]
    Map<String, WeightedSelector> openPunctSel = [:]
    Map<String, WeightedSelector> closePunctSel = [:]

    int contextDepth = 3

    void loadWordModel(Config cfg) {
        println "📂 Loading Word Model..."
        def json = new JsonSlurper().parse(new File(cfg.wordStatsFile))
        if (json.vocabulary) loadedVocabulary = new HashSet<>(json.vocabulary)
        Map<String, Integer> rawLengthStats = json.lengthStartStats
        if (cfg.pruneMinTokens > 0) {
            rawLengthStats = rawLengthStats.findAll { k, v -> k.split(':')[0].toInteger() > cfg.pruneMinTokens }
            if (rawLengthStats.isEmpty()) { println "❌ Error: Pruning too aggressive."; System.exit(1) }
        }
        mainLengthSelector = new WeightedSelector(rawLengthStats)
        json.lengthStartStats.each { k, v ->
            def parts = k.split(':'); lengthToStartTypeSel.computeIfAbsent(parts[0], {[:]}).put(parts[1], v)
        }
        lengthToStartTypeSel.each { k, v -> lengthToStartTypeSel[k] = new WeightedSelector(v) }
        def build = { src, dest -> src.each { k1, inner -> dest[k1] = [:]; inner.each { k2, map -> dest[k1][k2] = new WeightedSelector(map) } } }
        build(json.startTokens, startSel); build(json.innerTokens, innerSel); build(json.lastTokens, lastSel)
        build(json.bigramStart, bigramStartSel); build(json.bigramInner, bigramInnerSel); build(json.bigramLast, bigramLastSel)
        build(json.trigramInner, trigramInnerSel); build(json.trigramLast, trigramLastSel)
    }

    void loadRhythmModel(String path) {
        println "📂 Loading Rhythm Model (Markov)..."
        def json = new JsonSlurper().parse(new File(path))

        json.markovChain.each { ctx, nextMap -> markovSelectors[ctx] = new WeightedSelector(nextMap) }
        lengthStats = json.lengthStats
        segmentStarts = json.segmentStarts
        json.openPunctStats.each { k, v -> openPunctSel[k] = new WeightedSelector(v) }
        json.closePunctStats.each { k, v -> closePunctSel[k] = new WeightedSelector(v) }
        json.transitions.each { k, v -> transitionSel[k] = new WeightedSelector(v) }
    }

    // --- STRUCTURAL GENERATION ---

    Segment generateStructure(String type, int depth) {
        Segment seg = new Segment(type: type)

        if (openPunctSel[type]) seg.openPunct = openPunctSel[type].select(Globals.RND)
        if (closePunctSel[type]) seg.closePunct = closePunctSel[type].select(Globals.RND)

        def lens = lengthStats[type]
        if (!lens) return seg
        int targetLen = lens[Globals.RND.nextInt(lens.size())]

        def starts = segmentStarts[type]
        if (!starts) return seg
        String startCtx = starts[Globals.RND.nextInt(starts.size())]
        List<String> context = startCtx.split(/\|/).toList()

        context.each { token -> applyToken(seg, token, depth) }
        int currentLen = context.size()

        while (true) {
            String key = "${type}|" + context.takeRight(contextDepth).join("|")
            def sel = markovSelectors[key]
            if (!sel) break

            String nextToken = sel.select(Globals.RND)

            if (nextToken.startsWith("PAUSE") && context.size() > 0 && context.last().startsWith("PAUSE")) {
                break
            }

            if (currentLen >= targetLen * 1.5) {
                if (sel.keys.contains("END")) nextToken = "END"
                else if (nextToken.startsWith("PAUSE") || nextToken.startsWith("SEGMENT")) break
            }
            if (nextToken == "END") break

            applyToken(seg, nextToken, depth)

            if (nextToken == "SEGMENT:ATTRIBUTION") {
                break
            }

            context << nextToken
            currentLen++
            if (currentLen > 100) break
        }
        return seg
    }

    void applyToken(Segment seg, String token, int depth) {
        def parts = token.split(':')
        def t = parts[0]
        def v = parts.size() > 1 ? parts[1] : ""

        if (t == "WORD") {
            int len = 0
            if (v == "SHORT") len = Globals.RND.nextInt(3) + 1
            else if (v == "MED") len = Globals.RND.nextInt(4) + 4
            else len = Globals.RND.nextInt(5) + 8
            seg.wordLengths << len
        } else if (t == "PAUSE") {
            seg.children << new Segment(type: 'PAUSE', terminator: v, childInsertPosition: seg.wordLengths.size())
        } else if (t == "SEGMENT") {
            if (depth < 4) {
                Segment child = generateStructure(v, depth + 1)
                child.childInsertPosition = seg.wordLengths.size()
                seg.children << child
            }
        }
    }

    String generateWord(int targetLen, int ngramMode) {
        def lenKey = targetLen.toString()
        def typeSel = lengthToStartTypeSel[lenKey]
        if (!typeSel) {
            def fused = mainLengthSelector.select(Globals.RND)
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

    String renderSegment(Segment seg, Config cfg, Map stats) {
        def sb = new StringBuilder()
        if (seg.openPunct) sb.append(seg.openPunct)

        boolean isAttrib = (seg.type == 'ATTRIBUTION')

        def childIdx = 0
        def sortedChildren = seg.children.sort { it.childInsertPosition }

        seg.wordLengths.eachWithIndex { len, i ->
            while (childIdx < sortedChildren.size() && sortedChildren[childIdx].childInsertPosition == i) {
                def child = sortedChildren[childIdx]
                if (child.type == 'PAUSE') {
                    if (sb.length() > 0 && sb.charAt(sb.length() - 1) == ' ') sb.setLength(sb.length() - 1)
                    sb.append(child.terminator).append(' ')
                } else {
                    def out = renderSegment(child, cfg, stats)
                    if (out) sb.append(out).append(' ')
                }
                childIdx++
            }

            int currentMode = cfg.ngramMode
            String w = generateWord(len, currentMode)
            boolean forced = false
            if (cfg.uniqueMode && loadedVocabulary) {
                int retries = 0; int l1=20; int l2=40; int l3=60
                while (loadedVocabulary.contains(w)) {
                    stats.filteredCount++
                    retries++
                    if (retries>=l1 && retries<l2) currentMode=2
                    else if (retries>=l2 && retries<l3) currentMode=1
                    else if (retries>=l3) { stats.forcedCount++; forced=true; break }
                    w = generateWord(len, currentMode)
                }
            }

            if (forced) w = "[${w}]"

            // SPECIAL: Title Case for Attribution Words
            if (isAttrib) {
                w = w[0].toUpperCase() + (w.length()>1 ? w.substring(1) : "")
            }

            if (isAttrib && i == 0) {
                // No space before first word in attribution
            }

            sb.append(w).append(' ')
        }

        while (childIdx < sortedChildren.size()) {
            def child = sortedChildren[childIdx]
            if (child.type != 'PAUSE') {
                def out = renderSegment(child, cfg, stats)
                if (out) sb.append(out).append(' ')
            }
            childIdx++
        }

        if (sb.length() > 0 && sb.charAt(sb.length() - 1) == ' ') sb.setLength(sb.length() - 1)

        if (seg.terminator && seg.type != 'SENTENCE' && seg.type != 'ATTRIBUTION') sb.append(seg.terminator)
        if (seg.closePunct) sb.append(seg.closePunct)
        return sb.toString()
    }

    void execute(Config cfg) {
        if (cfg.uniqueMode && !loadedVocabulary) { println "❌ Error: Unique Mode requires vocabulary."; System.exit(1) }
        if (cfg.uniqueMode) println "✨ Unique Filter: ${loadedVocabulary.size()} words loaded."

        PrintStream outStream = System.out
        if (cfg.f_File) { outStream = new PrintStream(new File(cfg.f_File)); println "✍️  Writing to file: ${cfg.f_File}" }
        else { println "\n✍️  OUTPUT:"; println "=" * 60 }

        int generatedCount = 0
        def stats = [filteredCount: 0, forcedCount: 0]

        while (generatedCount < cfg.count) {
            if (cfg.sentenceStatsFile) {
                Segment root = generateStructure("SENTENCE", 0)

                boolean hasAttribution = root.children.any { it.type == 'ATTRIBUTION' }
                if (!hasAttribution) {
                    root.terminator = transitionSel["TERM"]?.select(Globals.RND) ?: "."
                    if (root.terminator == '\u00B6') root.terminator = "."
                } else {
                    root.terminator = "" // Attribution IS the terminator
                }

                String output = renderSegment(root, cfg, stats)
                if (output) {
                    // Smart Capitalization
                    output = Utils.smartCaps(output)

                    if (root.terminator) output += root.terminator
                }
                outStream.println(output)
                generatedCount++
            } else {
                def fused = mainLengthSelector.select(Globals.RND)
                if (!fused) break
                def targetLen = fused.split(':')[0].toInteger()
                int currentMode = cfg.ngramMode
                String w = generateWord(targetLen, currentMode)
                boolean accepted = true
                if (cfg.uniqueMode) {
                    int retries = 0
                    while (loadedVocabulary.contains(w)) {
                        stats.filteredCount++; retries++
                        if (retries>=20 && retries<40) currentMode=2
                        else if (retries>=40 && retries<60) currentMode=1
                        else if (retries>=60) { accepted=false; break }
                        w = generateWord(targetLen, currentMode)
                    }
                }
                if (accepted) { outStream.println(w); generatedCount++ }
            }
        }
        if (cfg.f_File) outStream.close()
        println "=" * 60
        if (cfg.uniqueMode) {
            println "🔒 Filtered ${stats.filteredCount} duplicates."
            if (stats.forcedCount > 0) println "⚠️  Forced to accept ${stats.forcedCount} real words."
        }
        println "✅ Done."
    }
}