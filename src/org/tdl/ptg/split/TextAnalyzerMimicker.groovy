package org.tdl.ptg.split

import groovy.transform.Canonical
import groovy.json.JsonSlurper
import groovy.json.JsonOutput
import java.nio.file.Files
import java.nio.file.Paths

// ----------------------------------------------------------------
// --- SHARED GLOBALS & UTILS ---
// ----------------------------------------------------------------

class Globals {
    static final Random RND = new Random()
    static final Set<String> VOWELS = new HashSet(['a', 'e', 'i', 'o', 'u', 'y', 'æ', 'ø', 'å', 'ä', 'ö', 'ü', 'é'])
    static final Set<String> TERMINATORS = new HashSet(['.', '!', '?', '\u2026'])  // … = \u2026
    static final Set<String> PAUSES = new HashSet([',', ';', ':'])
    static final Set<String> ALL_PUNCT = new HashSet(['.', '!', '?', ',', ':', ';',
                                                      '\u2014', '-', '\u2026',  // — and …
                                                      '\u0022', '\u201C', '\u201D',  // " " "
                                                      '\u0027', '\u2018', '\u2019',  // ' ' '
                                                      '(', ')', '[', ']', '{', '}'])
    static final int MAX_EFF_LEN = 8
}

// ----------------------------------------------------------------
// --- PUNCTUATION & TOKEN HANDLING ---
// ----------------------------------------------------------------

@Canonical
class PunctToken {
    String type      // TERM, PAUSE, OPEN, CLOSE, LINK, APOSTROPHE, UNKNOWN
    String value     // the actual character(s)
    String context   // QUOTE, SQUOTE, PAREN, BRACKET, BRACE, DASH, ELLIPSIS, SENTENCE, CLAUSE
}

@Canonical
class Token {
    String type = 'WORD'   // WORD, OPEN, CLOSE, TERM, PAUSE, LINK, DISCARD
    String text = ''       // the word content (if any)
    String value = ''      // punctuation value (if any)
    String context = ''    // QUOTE, SQUOTE, PAREN, BRACKET, BRACE, etc.
    int wordLength = 0     // CV-token count for words
}

class Punctuation {
    // Track quote state for ambiguous straight quotes
    Map<String, Boolean> quoteState = ['"': false, "'": false]

    static boolean isApostrophe(String fullText, int pos) {
        if (pos <= 0 || pos >= fullText.length() - 1) return false
        def before = fullText[pos - 1]
        def after = fullText[pos + 1]
        // letter'letter pattern (don't, it's, O'Brien)
        if (before =~ /\w/ && after =~ /\w/) return true
        // 'digits pattern ('90s)
        if (pos < fullText.length() - 1 && fullText[pos + 1] =~ /\d/) return true
        return false
    }

    static PunctToken classifySingle(String s) {
        // Unicode constants for quotes
        // " = \u0022, " = \u201C, " = \u201D
        // ' = \u0027, ' = \u2018, ' = \u2019

        switch(s) {
        // Terminators
            case '.': return new PunctToken('TERM', s, 'SENTENCE')
            case '!': return new PunctToken('TERM', s, 'SENTENCE')
            case '?': return new PunctToken('TERM', s, 'SENTENCE')
            case '\u2026': return new PunctToken('TERM', s, 'ELLIPSIS')  // …

                // Pauses
            case ',': return new PunctToken('PAUSE', s, 'CLAUSE')
            case ';': return new PunctToken('PAUSE', s, 'CLAUSE')
            case ':': return new PunctToken('PAUSE', s, 'INTRO')

                // Links
            case '\u2014': return new PunctToken('LINK', s, 'DASH')  // —
            case '-': return new PunctToken('LINK', s, 'HYPHEN')

                // Unambiguous brackets
            case '(': return new PunctToken('OPEN', s, 'PAREN')
            case ')': return new PunctToken('CLOSE', s, 'PAREN')
            case '[': return new PunctToken('OPEN', s, 'BRACKET')
            case ']': return new PunctToken('CLOSE', s, 'BRACKET')
            case '{': return new PunctToken('OPEN', s, 'BRACE')
            case '}': return new PunctToken('CLOSE', s, 'BRACE')

                // Smart quotes (unambiguous)
            case '\u201C': return new PunctToken('OPEN', s, 'QUOTE')    // "
            case '\u201D': return new PunctToken('CLOSE', s, 'QUOTE')   // "
            case '\u2018': return new PunctToken('OPEN', s, 'SQUOTE')   // '
            case '\u2019': return new PunctToken('CLOSE', s, 'SQUOTE') // '

                // Straight quotes (ambiguous - need context)
            case '\u0022': return new PunctToken('UNKNOWN', s, 'QUOTE')  // "
            case '\u0027': return new PunctToken('UNKNOWN', s, 'SQUOTE') // '

            default: return null
        }
    }

    // Guess open/close for ambiguous quotes based on neighbors
    static String guessOpenClose(String prevText, String nextText) {
        def prevEndsWord = prevText && prevText =~ /[\w.,!?]$/
        def prevEndsSpace = !prevText || prevText =~ /[\s(\[{—]$/
        def nextStartsWord = nextText && nextText =~ /^\w/
        def nextStartsSpace = !nextText || nextText =~ /^[\s)\]}.,:;!?—]/

        if (prevEndsSpace && nextStartsWord) return 'OPEN'
        if (prevEndsWord && nextStartsSpace) return 'CLOSE'
        return 'UNKNOWN'  // Still ambiguous
    }

    void resetQuoteState() {
        quoteState.replaceAll { k, v -> false }
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

    // Normalize straight quotes to smart quotes using heuristics
    static String normalizeQuotes(String text) {
        def result = new StringBuilder()
        def chars = text.toCharArray()

        // Unicode: " = \u0022, " = \u201C, " = \u201D
        //          ' = \u0027, ' = \u2018, ' = \u2019
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
                // Check for apostrophe first
                if (Punctuation.isApostrophe(text, i)) {
                    result.append(CLOSE_SINGLE)  // Apostrophe uses closing single quote
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

    // Normalize ellipsis variations
    static String normalizeEllipsis(String text) {
        return text.replaceAll(/\.{3,}/, '\u2026').replaceAll(/\s*\u2026\s*/, '\u2026 ')
    }

    // Normalize dashes
    static String normalizeDashes(String text) {
        return text.replaceAll(/--+/, '\u2014').replaceAll(/\s*\u2014\s*/, ' \u2014 ')
    }
}

// ----------------------------------------------------------------
// --- HIERARCHICAL SEGMENT MODEL ---
// ----------------------------------------------------------------

@Canonical
class Segment {
    String type = 'SENTENCE'    // SENTENCE, CLAUSE, PAREN, BRACKET, BRACE, QUOTE, SQUOTE, DASH_ASIDE, ATTRIBUTION
    List<Integer> wordLengths = []
    List<Segment> children = []
    int childInsertPosition = -1  // Where in wordLengths the child was inserted
    String openPunct = ''
    String closePunct = ''
    String terminator = ''

    Segment copy() {
        def s = new Segment(type: type, openPunct: openPunct, closePunct: closePunct,
                terminator: terminator, childInsertPosition: childInsertPosition)
        s.wordLengths = new ArrayList(wordLengths)
        s.children = children.collect { it.copy() }
        return s
    }

    Map toMap() {
        return [
                type: type,
                wordLengths: wordLengths,
                children: children.collect { it.toMap() },
                childInsertPosition: childInsertPosition,
                openPunct: openPunct,
                closePunct: closePunct,
                terminator: terminator
        ]
    }

    static Segment fromMap(Map m) {
        def s = new Segment(
                type: m.type ?: 'SENTENCE',
                wordLengths: m.wordLengths ?: [],
                childInsertPosition: m.childInsertPosition ?: -1,
                openPunct: m.openPunct ?: '',
                closePunct: m.closePunct ?: '',
                terminator: m.terminator ?: ''
        )
        s.children = (m.children ?: []).collect { Segment.fromMap(it) }
        return s
    }

    // Validation: check if this segment has balanced quotes
    boolean hasBalancedQuotes() {
        if (type in ['QUOTE', 'SQUOTE']) {
            return openPunct && closePunct
        }
        return children.every { it.hasBalancedQuotes() }
    }
}

// ----------------------------------------------------------------
// --- STRUCTURE TOKENIZER ---
// ----------------------------------------------------------------

class StructureTokenizer {

    List<Token> tokenize(String text) {
        // Normalize first
        def normalized = Utils.normalizeEllipsis(Utils.normalizeDashes(Utils.normalizeQuotes(text)))

        def tokens = []
        def words = normalized.split(/\s+/)

        words.each { word ->
            if (!word) return
            tokens.addAll(tokenizeWord(word))
        }

        return tokens
    }

    List<Token> tokenizeWord(String word) {
        def tokens = []
        def current = new StringBuilder()
        def chars = word.toCharArray()

        int i = 0
        while (i < chars.length) {
            def c = chars[i].toString()

            // Check for ellipsis
            if (c == '\u2026') {  // …
                if (current.length() > 0) {
                    tokens << createWordToken(current.toString())
                    current = new StringBuilder()
                }
                tokens << createPunctToken(Punctuation.classifySingle(c))
                i++
                continue
            }

            def punct = Punctuation.classifySingle(c)
            if (punct) {
                if (current.length() > 0) {
                    tokens << createWordToken(current.toString())
                    current = new StringBuilder()
                }
                tokens << createPunctToken(punct)
            } else {
                current.append(c)
            }
            i++
        }

        if (current.length() > 0) {
            tokens << createWordToken(current.toString())
        }

        return tokens
    }

    Token createWordToken(String text) {
        def struct = Utils.tokenizeStructure(text)
        def hasVowel = text.toLowerCase().any { Globals.VOWELS.contains(it.toString()) }
        if (!struct || !hasVowel) return null
        return new Token(type: 'WORD', text: text, wordLength: struct.size())
    }

    Token createPunctToken(PunctToken p) {
        if (!p) return null
        return new Token(
                type: p.type,
                value: p.value,
                context: p.context
        )
    }
}

// ----------------------------------------------------------------
// --- STRUCTURE BALANCER ---
// ----------------------------------------------------------------

class StructureBalancer {

    List<Token> balance(List<Token> tokens) {
        // First pass: resolve UNKNOWN quotes using context
        resolveUnknownQuotes(tokens)

        // Second pass: balance and discard orphans
        balanceStructures(tokens)

        return tokens.findAll { it.type != 'DISCARD' }
    }

    void resolveUnknownQuotes(List<Token> tokens) {
        tokens.eachWithIndex { tok, i ->
            if (tok.type == 'UNKNOWN') {
                def prevText = ''
                def nextText = ''

                // Look backwards for text
                for (int j = i - 1; j >= 0; j--) {
                    if (tokens[j].type == 'WORD') {
                        prevText = tokens[j].text
                        break
                    } else if (tokens[j].value) {
                        prevText = tokens[j].value
                        break
                    }
                }

                // Look forwards for text
                for (int j = i + 1; j < tokens.size(); j++) {
                    if (tokens[j].type == 'WORD') {
                        nextText = tokens[j].text
                        break
                    } else if (tokens[j].value) {
                        nextText = tokens[j].value
                        break
                    }
                }

                def guess = Punctuation.guessOpenClose(prevText, nextText)
                tok.type = (guess == 'OPEN') ? 'OPEN' : 'CLOSE'
            }
        }
    }

    void balanceStructures(List<Token> tokens) {
        // Process each sentence separately (TERM tokens are barriers)
        // But include CLOSE tokens that immediately follow a TERM (for patterns like .")
        def sentenceStart = 0

        for (int i = 0; i <= tokens.size(); i++) {
            // Check if this is a TERM
            if (i < tokens.size() && tokens[i].type == 'TERM') {
                // Find the actual sentence end - include any CLOSE tokens that follow
                int sentenceEnd = i + 1  // Start after the TERM
                while (sentenceEnd < tokens.size() && tokens[sentenceEnd].type == 'CLOSE') {
                    sentenceEnd++
                }

                if (sentenceEnd > sentenceStart) {
                    balanceSentence(tokens, sentenceStart, sentenceEnd)
                }
                sentenceStart = sentenceEnd
            }
            // Also handle end of tokens
            else if (i == tokens.size() && i > sentenceStart) {
                balanceSentence(tokens, sentenceStart, i)
            }
        }
    }

    void balanceSentence(List<Token> tokens, int start, int end) {
        ['QUOTE', 'SQUOTE', 'PAREN', 'BRACKET', 'BRACE'].each { context ->
            def stack = []  // Stack of indices of OPEN tokens

            for (int i = start; i < end; i++) {
                def tok = tokens[i]
                if (tok.type == 'DISCARD') continue

                if (tok.type == 'OPEN' && tok.context == context) {
                    stack.push(i)
                }
                else if (tok.type == 'CLOSE' && tok.context == context) {
                    if (stack) {
                        stack.pop()  // Matched
                    } else {
                        tok.type = 'DISCARD'  // Unmatched close
                    }
                }
            }

            // Handle unmatched opens within this sentence
            stack.reverse().each { openIdx ->
                discardOrphanOpenInRange(tokens, openIdx, start, end)
            }
        }
    }

    void discardOrphanOpenInRange(List<Token> tokens, int openIdx, int start, int end) {
        // Count non-discarded tokens before and after within the sentence range
        def beforeCount = 0
        def afterCount = 0

        for (int i = start; i < openIdx; i++) {
            if (tokens[i].type != 'DISCARD') beforeCount++
        }
        for (int i = openIdx + 1; i < end; i++) {
            if (tokens[i].type != 'DISCARD') afterCount++
        }

        if (afterCount < beforeCount) {
            // Discard from open to end of sentence
            for (int i = openIdx; i < end; i++) {
                if (tokens[i].type != 'DISCARD') {
                    tokens[i].type = 'DISCARD'
                }
            }
        } else {
            // Just discard the orphan open
            tokens[openIdx].type = 'DISCARD'
        }
    }
}

// ----------------------------------------------------------------
// --- STRUCTURE ANALYZER ---
// ----------------------------------------------------------------

class StructureAnalyzer {
    StructureTokenizer tokenizer = new StructureTokenizer()
    StructureBalancer balancer = new StructureBalancer()

    List<Segment> parseToSegments(List<Token> tokens) {
        def segments = []
        def stack = [new Segment(type: 'SENTENCE')]
        def lastWasPause = false
        def justClosedQuote = false  // Track if we just closed a quote

        tokens.eachWithIndex { tok, idx ->
            if (stack.isEmpty()) {
                stack = [new Segment(type: 'SENTENCE')]
            }
            // Note: Groovy's push() prepends to index 0, so first() gets the top of stack
            def current = stack.first()

            switch (tok.type) {
                case 'WORD':
                    if (tok.wordLength > 0) {
                        current.wordLengths << tok.wordLength
                        lastWasPause = false
                        justClosedQuote = false
                    }
                    break

                case 'OPEN':
                    def child = new Segment(
                            type: tok.context,
                            openPunct: tok.value,
                            childInsertPosition: current.wordLengths.size()
                    )
                    current.children << child
                    stack.push(child)  // push prepends to front
                    lastWasPause = false
                    justClosedQuote = false
                    break

                case 'CLOSE':
                    if (stack.size() > 1 && stack.first().type == tok.context) {
                        stack.first().closePunct = tok.value
                        stack.pop()  // pop removes from front
                        justClosedQuote = (tok.context in ['QUOTE', 'SQUOTE'])
                    }
                    lastWasPause = false
                    break

                case 'TERM':
                    // Check if this terminator is inside a quote (quote + terminator pattern like .")
                    if (stack.size() > 1 && stack.first().type in ['QUOTE', 'SQUOTE']) {
                        // Terminator inside quote - store it with the quote
                        stack.first().terminator = tok.value
                    } else {
                        // Close all open structures and complete sentence
                        while (stack.size() > 1) {
                            stack.pop()
                        }
                        current = stack.first()
                        current.terminator = tok.value
                        if (current.wordLengths || current.children) {
                            segments << current
                        }
                        stack = [new Segment(type: 'SENTENCE')]
                    }
                    lastWasPause = false
                    justClosedQuote = false
                    break

                case 'PAUSE':
                    if (current.wordLengths && !lastWasPause) {
                        current.children << new Segment(
                                type: 'PAUSE',
                                terminator: tok.value,
                                childInsertPosition: current.wordLengths.size()
                        )
                        lastWasPause = true
                    }
                    justClosedQuote = false
                    break

                case 'LINK':
                    if (tok.context == 'DASH') {
                        // Check for attribution pattern: just closed a quote, now seeing em-dash
                        if (justClosedQuote && stack.size() >= 1) {
                            // This is an attribution dash - start attribution segment
                            def attr = new Segment(
                                    type: 'ATTRIBUTION',
                                    openPunct: tok.value,
                                    childInsertPosition: current.wordLengths.size()
                            )
                            current.children << attr
                            stack.push(attr)
                        } else {
                            // Regular dash-aside handling
                            def asideIdx = stack.findIndexOf { it.type == 'DASH_ASIDE' && !it.closePunct }
                            if (asideIdx >= 0) {
                                stack[asideIdx].closePunct = tok.value
                                while (asideIdx > 0) {
                                    stack.pop()
                                    asideIdx--
                                }
                                stack.pop()  // Remove the aside itself
                            } else if (stack.size() > 0) {
                                def aside = new Segment(
                                        type: 'DASH_ASIDE',
                                        openPunct: tok.value,
                                        childInsertPosition: current.wordLengths.size()
                                )
                                current.children << aside
                                stack.push(aside)
                            }
                        }
                        lastWasPause = false
                    }
                    justClosedQuote = false
                    break
            }
        }

        // Handle any remaining content without terminator
        if (stack) {
            // Close any open attribution (they don't have closing punct)
            while (stack.size() > 1 && stack.first().type == 'ATTRIBUTION') {
                stack.pop()
            }

            def remaining = stack.find { it.type == 'SENTENCE' }
            if (remaining && (remaining.wordLengths || remaining.children)) {
                // Don't add default terminator if we have an attribution (it ends the line)
                def hasAttribution = remaining.children.any { it.type == 'ATTRIBUTION' }
                if (!hasAttribution && !remaining.terminator) {
                    remaining.terminator = '.'
                }
                segments << remaining
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
            case '-2':
            case '-b': cfg.ngramMode = 2; break
            case '-3':
            case '-t': cfg.ngramMode = 3; break
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

// ----------------------------------------------------------------
// --- WEIGHTED SELECTOR ---
// ----------------------------------------------------------------

@Canonical
class WeightedSelector {
    List<String> keys = []
    List<Long> cumulativeWeights = []
    long totalWeight = 0

    WeightedSelector(Map<String, Integer> map) {
        long runningTotal = 0L
        map.sort { -it.value }.each { key, weight ->
            if (weight > 0) {
                this.keys.add(key)
                runningTotal += weight
                this.cumulativeWeights.add(runningTotal)
            }
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
// --- ANALYZER ---
// ----------------------------------------------------------------

class Analyzer {
    StructureTokenizer tokenizer = new StructureTokenizer()
    StructureBalancer balancer = new StructureBalancer()
    StructureAnalyzer structureAnalyzer = new StructureAnalyzer()

    // Word-level stats (unchanged)
    Map<String, Integer> lengthStartStats = [:]
    Map<String, Map<String, Map<String, Integer>>> startTokens = [:]
    Map<String, Map<String, Map<String, Integer>>> innerTokens = [:]
    Map<String, Map<String, Map<String, Integer>>> lastTokens = [:]
    Map<String, Map<String, Map<String, Integer>>> bigramStart = [:]
    Map<String, Map<String, Map<String, Integer>>> bigramInner = [:]
    Map<String, Map<String, Map<String, Integer>>> bigramLast = [:]
    Map<String, Map<String, Map<String, Integer>>> trigramInner = [:]
    Map<String, Map<String, Map<String, Integer>>> trigramLast = [:]

    // Sentence-level stats (enhanced)
    List<Map> segmentTemplates = []
    Map<String, Map<String, Integer>> transitions = [:]
    Map<String, Map<String, Integer>> nestingPatterns = [:]  // What structures contain what

    Set<String> vocabulary = new HashSet<>()

    void process(Config cfg) {
        println "📖 Analyzing source..."

        def fileContent = new File(cfg.f_File).getText("UTF-8")
        if (fileContent.length() > 0 && fileContent.charAt(0) == '\uFEFF') fileContent = fileContent.substring(1)

        def text = fileContent.replaceAll(/\r\n/, "\n").replaceAll(/\s+/, " ")

        // Tokenize and balance
        def tokens = tokenizer.tokenize(text)
        tokens = tokens.findAll { it != null }
        tokens = balancer.balance(tokens)

        int wordsProcessed = 0
        int segmentsProcessed = 0

        // Process word-level statistics from tokens
        tokens.findAll { it.type == 'WORD' && it.wordLength > 0 }.each { tok ->
            def wordPart = tok.text
            def cleanPart = wordPart.findAll { Character.isLetter(it as char) }.join('').toLowerCase()
            def struct = Utils.tokenizeStructure(wordPart)

            if (struct) {
                wordsProcessed++
                if (cfg.uniqueMode) vocabulary.add(cleanPart)

                def len = struct.size()
                def effLen = Math.min(len, Globals.MAX_EFF_LEN).toString()
                def startType = struct[0][0]
                def fusedKey = "${len}:${startType}"
                lengthStartStats[fusedKey] = (lengthStartStats[fusedKey] ?: 0) + 1

                struct.eachWithIndex { t, idx ->
                    def content = t[1]
                    if (idx == 0) {
                        Utils.ensureMap(startTokens, effLen, startType)[content] =
                                (Utils.ensureMap(startTokens, effLen, startType)[content] ?: 0) + 1
                    } else {
                        def prev = struct[idx-1]
                        def prevContent = prev[1]
                        def prevType = prev[0]
                        def baseMap = (idx == len - 1) ? lastTokens : innerTokens
                        Utils.ensureMap(baseMap, effLen, prevType)[content] =
                                (Utils.ensureMap(baseMap, effLen, prevType)[content] ?: 0) + 1

                        def bgMap = (idx == len - 1) ? bigramLast : (idx == 1) ? bigramStart : bigramInner
                        Utils.ensureMap(bgMap, effLen, prevContent)[content] =
                                (Utils.ensureMap(bgMap, effLen, prevContent)[content] ?: 0) + 1

                        if (idx >= 2) {
                            def prev2 = struct[idx-2][1]
                            def tgKey = "${prev2}:${prevContent}"
                            def tgMap = (idx == len - 1) ? trigramLast : trigramInner
                            Utils.ensureMap(tgMap, effLen, tgKey)[content] =
                                    (Utils.ensureMap(tgMap, effLen, tgKey)[content] ?: 0) + 1
                        }
                    }
                }
            }
        }

        // Process sentence-level structures
        if (cfg.sentenceStatsFile) {
            def segments = structureAnalyzer.parseToSegments(tokens)
            segments.each { segment ->
                if (segment.wordLengths || segment.children) {
                    segmentTemplates << segment.toMap()
                    segmentsProcessed++

                    // Track transitions
                    def termKey = segment.terminator ?: '.'
                    transitions.computeIfAbsent('TERM', {[:]}).merge(termKey, 1, Integer::sum)

                    // Track nesting patterns
                    segment.children.each { child ->
                        def nestKey = "${segment.type}->${child.type}"
                        nestingPatterns.computeIfAbsent(segment.type, {[:]}).merge(child.type, 1, Integer::sum)
                    }
                }
            }
        }

        println "\n📊 Analysis Stats:"
        println "   Words Processed: ${wordsProcessed}"
        if (cfg.uniqueMode) println "   Unique Vocabulary: ${vocabulary.size()} words stored"
        println "   Length Templates: ${lengthStartStats.size()}"
        if (cfg.sentenceStatsFile) {
            println "   Sentence Templates: ${segmentsProcessed}"
            println "   Nesting Patterns: ${nestingPatterns.size()} parent types"
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
                    segmentTemplates: segmentTemplates,
                    transitions: transitions,
                    nestingPatterns: nestingPatterns
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
    List<Segment> templates = []
    Map<String, WeightedSelector> transitionSel = [:]
    Map<String, WeightedSelector> nestingSel = [:]

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
            def parts = k.split(':')
            lengthToStartTypeSel.computeIfAbsent(parts[0], {[:]}).put(parts[1], v)
        }
        lengthToStartTypeSel.each { k, v -> lengthToStartTypeSel[k] = new WeightedSelector(v) }

        def build = { src, dest ->
            src.each { k1, inner -> dest[k1] = [:]
                inner.each { k2, map -> dest[k1][k2] = new WeightedSelector(map) }
            }
        }
        build(json.startTokens, startSel)
        build(json.innerTokens, innerSel)
        build(json.lastTokens, lastSel)
        build(json.bigramStart, bigramStartSel)
        build(json.bigramInner, bigramInnerSel)
        build(json.bigramLast, bigramLastSel)
        build(json.trigramInner, trigramInnerSel)
        build(json.trigramLast, trigramLastSel)
    }

    void loadRhythmModel(String path) {
        println "📂 Loading Rhythm Model..."
        def json = new JsonSlurper().parse(new File(path))

        def allTemplates = (json.segmentTemplates ?: []).collect { Segment.fromMap(it) }

        // Filter out malformed templates
        this.templates = allTemplates.findAll { t ->
            // Reject templates where QUOTE children have no words (broken structure)
            def hasEmptyQuote = t.children.any { c ->
                c.type in ['QUOTE', 'SQUOTE'] && c.wordLengths.isEmpty() && c.children.isEmpty()
            }
            if (hasEmptyQuote) return false

            // Reject templates with unbalanced quotes
            if (!t.hasBalancedQuotes()) return false

            // Reject templates with consecutive QUOTE children (no content between them)
            def quoteChildren = t.children.findAll { it.type in ['QUOTE', 'SQUOTE'] }
            if (quoteChildren.size() > 1) {
                // Check if any two quotes have the same insert position (consecutive)
                def positions = quoteChildren.collect { it.childInsertPosition }
                // Also check if quotes are at adjacent positions with no words between
                for (int i = 0; i < quoteChildren.size() - 1; i++) {
                    def pos1 = quoteChildren[i].childInsertPosition
                    def pos2 = quoteChildren[i + 1].childInsertPosition
                    // If two quotes are at the same position, or one ends where the next begins
                    // with no sentence words between, reject
                    if (pos1 == pos2) return false
                }
            }

            return true
        }

        def rejected = allTemplates.size() - this.templates.size()
        if (rejected > 0) {
            println "   \u26A0\uFE0F  Filtered ${rejected} malformed templates"
        }

        (json.transitions ?: [:]).each { k, v ->
            transitionSel[k] = new WeightedSelector(v)
        }

        (json.nestingPatterns ?: [:]).each { k, v ->
            nestingSel[k] = new WeightedSelector(v)
        }
    }

    String generateWord(int targetLen, int ngramMode) {
        def lenKey = targetLen.toString()
        def typeSel = lengthToStartTypeSel[lenKey]

        if (!typeSel) {
            def fused = mainLengthSelector.select(Globals.RND)
            if (!fused) return "blob"
            lenKey = fused.split(':')[0]
            targetLen = lenKey.toInteger()
            typeSel = new WeightedSelector([(fused.split(':')[1]): 1])
        }

        def startType = typeSel.select(Globals.RND)
        def effLen = Math.min(targetLen, Globals.MAX_EFF_LEN).toString()

        def tokens = []
        def startContent = startSel[effLen]?[startType]?.select(Globals.RND) ?: "err"
        tokens << startContent
        def prevType = startType

        for (int i = 1; i < targetLen; i++) {
            def isLast = (i == targetLen - 1)
            def token = null

            if (ngramMode >= 3 && tokens.size() >= 2) {
                def tgKey = "${tokens[-2]}:${tokens[-1]}"
                def sel = isLast ? trigramLastSel : trigramInnerSel
                token = sel[effLen]?[tgKey]?.select(Globals.RND)
            }
            if (!token && ngramMode >= 2) {
                def bgKey = tokens[-1]
                def sel = isLast ? bigramLastSel : (i == 1 ? bigramStartSel : bigramInnerSel)
                token = sel[effLen]?[bgKey]?.select(Globals.RND)
            }
            if (!token) {
                def sel = isLast ? lastSel : innerSel
                token = sel[effLen]?[prevType]?.select(Globals.RND)
            }

            if (!token) token = (prevType == 'C') ? 'a' : 'b'
            tokens << token
            prevType = (prevType == 'C' ? 'V' : 'C')
        }
        return tokens.join('')
    }

    String generateSegment(Segment template, int ngramMode, Config cfg, Map stats) {
        def sb = new StringBuilder()

        // Opening punctuation (for quotes, parens, etc.)
        if (template.openPunct) {
            sb.append(template.openPunct)
            // No space after opening quote/paren
        }

        // Track which children we've processed
        def childIdx = 0
        def sortedChildren = template.children.sort { it.childInsertPosition }

        // Generate words, inserting children at appropriate positions
        template.wordLengths.eachWithIndex { len, i ->
            // Check if any children should be inserted before this word
            while (childIdx < sortedChildren.size() &&
                    sortedChildren[childIdx].childInsertPosition == i) {
                def child = sortedChildren[childIdx]
                if (child.type == 'PAUSE') {
                    // Remove trailing space before pause punctuation
                    if (sb.length() > 0 && sb.charAt(sb.length() - 1) == ' ') {
                        sb.setLength(sb.length() - 1)
                    }
                    sb.append(child.terminator)
                    sb.append(' ')
                } else if (child.type in ['QUOTE', 'SQUOTE', 'PAREN', 'BRACKET', 'BRACE', 'DASH_ASIDE', 'ATTRIBUTION']) {
                    // Nested structure - generate recursively
                    def childOutput = generateSegment(child, ngramMode, cfg, stats)
                    if (childOutput) {
                        sb.append(childOutput)
                        sb.append(' ')
                    }
                }
                childIdx++
            }

            // Generate word
            int currentMode = ngramMode
            String w = generateWord(len, currentMode)
            boolean forced = false

            if (cfg.uniqueMode && loadedVocabulary) {
                int retries = 0
                int limitTrigram = 20
                int limitBigram = 40
                int limitUnigram = 60

                while (loadedVocabulary.contains(w)) {
                    stats.filteredCount = (stats.filteredCount ?: 0) + 1
                    retries++

                    if (retries >= limitTrigram && retries < limitBigram) currentMode = 2
                    else if (retries >= limitBigram && retries < limitUnigram) currentMode = 1
                    else if (retries >= limitUnigram) {
                        stats.forcedCount = (stats.forcedCount ?: 0) + 1
                        forced = true
                        break
                    }
                    w = generateWord(len, currentMode)
                }
            }

            if (forced) {
                sb.append("[${w}]")
            } else {
                sb.append(w)
            }
            sb.append(' ')
        }

        // Any remaining children after all words
        while (childIdx < sortedChildren.size()) {
            def child = sortedChildren[childIdx]
            if (child.type == 'PAUSE') {
                // Skip trailing pauses
            } else if (child.type in ['QUOTE', 'SQUOTE', 'PAREN', 'BRACKET', 'BRACE', 'DASH_ASIDE', 'ATTRIBUTION']) {
                def childOutput = generateSegment(child, ngramMode, cfg, stats)
                if (childOutput) {
                    sb.append(childOutput)
                    sb.append(' ')
                }
            }
            childIdx++
        }

        // Trim trailing space before closing punctuation
        if (sb.length() > 0 && sb.charAt(sb.length() - 1) == ' ') {
            sb.setLength(sb.length() - 1)
        }

        // Handle terminator that's inside a quote (like .")
        if (template.terminator && template.type in ['QUOTE', 'SQUOTE']) {
            sb.append(template.terminator)
        }

        // Closing punctuation (for quotes, parens, etc.)
        if (template.closePunct && template.type != 'SENTENCE') {
            sb.append(template.closePunct)
        }

        // Sentence terminator (only for top-level sentences)
        // Skip if any child quote already has a terminator (to avoid ".")
        if (template.terminator && template.type == 'SENTENCE') {
            def childHasTerminator = template.children.any {
                it.type in ['QUOTE', 'SQUOTE'] && it.terminator
            }
            if (!childHasTerminator) {
                sb.append(template.terminator)
            }
        }

        return sb.toString()
    }

    void execute(Config cfg) {
        if (cfg.uniqueMode && !loadedVocabulary) {
            println "❌ Error: Unique Mode (-u) requested, but model contains no vocabulary list."
            println "   (Run analysis again with -u to capture it)."
            System.exit(1)
        }

        if (cfg.uniqueMode) println "✨ Unique Filter: ${loadedVocabulary.size()} words loaded."

        PrintStream outStream = System.out
        if (cfg.f_File) {
            outStream = new PrintStream(new File(cfg.f_File))
            println "✍️  Writing to file: ${cfg.f_File}"
        } else {
            println "\n✍️  OUTPUT:"
            println "=" * 60
        }

        int generatedCount = 0
        def stats = [filteredCount: 0, forcedCount: 0]

        while (generatedCount < cfg.count) {
            if (cfg.sentenceStatsFile && templates) {
                // --- SENTENCE MODE (enhanced) ---
                def template = templates[Globals.RND.nextInt(templates.size())].copy()

                def output = generateSegment(template, cfg.ngramMode, cfg, stats)

                // Capitalize first letter
                if (output) {
                    output = capitalizeFirst(output)
                }

                outStream.println(output)
                generatedCount++

            } else {
                // --- WORD LIST MODE (unchanged) ---
                def fused = mainLengthSelector.select(Globals.RND)
                if (!fused) break

                def targetLen = fused.split(':')[0].toInteger()
                int currentMode = cfg.ngramMode
                String w = generateWord(targetLen, currentMode)

                boolean accepted = true

                if (cfg.uniqueMode) {
                    int retries = 0
                    int limitTrigram = 20
                    int limitBigram = 40
                    int limitUnigram = 60

                    while (loadedVocabulary.contains(w)) {
                        stats.filteredCount++
                        retries++

                        if (retries >= limitTrigram && retries < limitBigram) currentMode = 2
                        else if (retries >= limitBigram && retries < limitUnigram) currentMode = 1
                        else if (retries >= limitUnigram) {
                            println "⚠️  Skipped a word (could not generate unique variation)."
                            accepted = false
                            break
                        }
                        w = generateWord(targetLen, currentMode)
                    }
                }

                if (accepted) {
                    outStream.println(w)
                    generatedCount++
                }
            }
        }

        if (cfg.f_File) outStream.close()

        println "=" * 60
        if (cfg.uniqueMode) {
            println "🔒 Filtered ${stats.filteredCount} duplicates."
            if (stats.forcedCount > 0) println "⚠️  Forced to accept ${stats.forcedCount} real words (marked in [Brackets])."
        }
        println "✅ Done."
    }

    String capitalizeFirst(String s) {
        if (!s) return s
        // Find first letter (skip opening punctuation)
        def chars = s.toCharArray()
        for (int i = 0; i < chars.length; i++) {
            if (Character.isLetter(chars[i])) {
                chars[i] = Character.toUpperCase(chars[i])
                break
            }
            // Handle bracketed words [word]
            if (chars[i] == '[' && i + 1 < chars.length && Character.isLetter(chars[i + 1])) {
                chars[i + 1] = Character.toUpperCase(chars[i + 1])
                break
            }
        }
        return new String(chars)
    }
}

// ----------------------------------------------------------------
// --- MAIN ---
// ----------------------------------------------------------------

def cfg = parseArgs(args)

if (!cfg.valid) {
    println "❌ Error: ${cfg.error}"
    println """
PSEUDO FACTORY - USAGE:
  1. ANALYZE: groovy script.groovy -a -f <source.txt> -w <words.json> [-s <sent.json>] [-u]
  2. GENERATE: groovy script.groovy -g -w <words.json> [-s <sent.json>] [-c <n>] [-u] [-p <n>]
  
OPTIONS:
  -a          Analysis mode
  -g          Generation mode
  -f <file>   Input file (analysis) or output file (generation)
  -w <file>   Word statistics JSON file
  -s <file>   Sentence/structure statistics JSON file
  -u          Unique mode (filter known words)
  -p <n>      Prune words with fewer than n tokens
  -c <n>      Generate n items (default: 20)
  -1/-2/-3    N-gram mode: unigram/bigram/trigram (default: 3)
  -b/-t       Aliases for -2 (bigram) and -3 (trigram)
"""
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