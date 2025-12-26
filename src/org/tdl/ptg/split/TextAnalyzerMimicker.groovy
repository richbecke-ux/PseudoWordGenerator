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
    String type      // WORD, TERM, PAUSE, OPEN, CLOSE, TOGGLE, OPEN_SQUOTE, CLOSE_SQUOTE
    String text = ''
    String value = ''
    String context = '' // QUOTE, PAREN, DASH, SENTENCE
    int wordLength = 0
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

    static String smartCaps(String s) {
        if (!s) return s
        // Skip over markers like <, {, [ to find the first letter
        def m = (s =~ /[a-zA-Z]/)
        if (m.find()) {
            int idx = m.start()
            return s.substring(0, idx) + s.substring(idx, idx+1).toUpperCase() + s.substring(idx+1)
        }
        return s
    }
}

// ----------------------------------------------------------------
// --- PARSER MODELS ---
// ----------------------------------------------------------------

@Canonical
class Segment {
    String type = 'SENTENCE' // SENTENCE, QUOTE, SQUOTE, PAREN, DASH
    List<Integer> wordLengths = []
    List<Segment> children = []
    int childInsertPosition = -1
    String terminator = ''
}

// ----------------------------------------------------------------
// --- TOKENIZER ---
// ----------------------------------------------------------------

class Tokenizer {

    // Helpers for the "Whitespace" rules
    boolean isBoundary(char c) {
        return Character.isWhitespace(c) || Globals.TERMINATORS.contains(c.toString()) || Globals.PAUSES.contains(c.toString()) || c == ')' || c == ']'
    }

    List<Token> tokenize(String text) {
        List<Token> tokens = []
        StringBuilder buffer = new StringBuilder()

        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i)
            String s = c.toString()

            // 1. Single Quote / Apostrophe Logic
            if (s == '\'') {
                boolean prevIsSpace = (i == 0) || isBoundary(text.charAt(i-1))
                boolean nextIsAlpha = (i < text.length() - 1) && Character.isLetterOrDigit(text.charAt(i+1))
                boolean prevIsAlpha = (i > 0) && !isBoundary(text.charAt(i-1))
                boolean nextIsSpace = (i == text.length() - 1) || isBoundary(text.charAt(i+1))

                // "Opening quotes always have whitespace before and an alphanumeric after."
                if (prevIsSpace && nextIsAlpha) {
                    if (buffer.length() > 0) { tokens << createWord(buffer); buffer = new StringBuilder() }
                    tokens << new Token(type: 'OPEN_SQUOTE', value: "'", context: 'SQUOTE')
                }
                // "Closing quotes always have non-whitespace before and whitespace after."
                else if (prevIsAlpha && nextIsSpace) {
                    if (buffer.length() > 0) { tokens << createWord(buffer); buffer = new StringBuilder() }
                    tokens << new Token(type: 'CLOSE_SQUOTE', value: "'", context: 'SQUOTE')
                }
                else {
                    buffer.append(c)
                }
            }
            // 2. Double Quotes (Generic Toggle)
            else if (s == '"') {
                if (buffer.length() > 0) { tokens << createWord(buffer); buffer = new StringBuilder() }
                tokens << new Token(type: 'TOGGLE', value: '"', context: 'QUOTE')
            }
            // 3. Em-Dash (Generic Toggle)
            else if (s == '\u2014') {
                if (buffer.length() > 0) { tokens << createWord(buffer); buffer = new StringBuilder() }
                tokens << new Token(type: 'TOGGLE', value: '\u2014', context: 'DASH')
            }
            // 4. Parentheses
            else if (s == '(' || s == '[') {
                if (buffer.length() > 0) { tokens << createWord(buffer); buffer = new StringBuilder() }
                tokens << new Token(type: 'OPEN', value: '(', context: 'PAREN')
            }
            else if (s == ')' || s == ']') {
                if (buffer.length() > 0) { tokens << createWord(buffer); buffer = new StringBuilder() }
                tokens << new Token(type: 'CLOSE', value: ')', context: 'PAREN')
            }
            // 5. Terminators
            else if (Globals.TERMINATORS.contains(s)) {
                if (buffer.length() > 0) { tokens << createWord(buffer); buffer = new StringBuilder() }
                tokens << new Token(type: 'TERM', value: s, context: 'SENTENCE')
            }
            // 6. Pauses
            else if (Globals.PAUSES.contains(s)) {
                if (buffer.length() > 0) { tokens << createWord(buffer); buffer = new StringBuilder() }
                tokens << new Token(type: 'PAUSE', value: s)
            }
            // 7. Whitespace
            else if (Character.isWhitespace(c)) {
                if (buffer.length() > 0) { tokens << createWord(buffer); buffer = new StringBuilder() }
            }
            // 8. Word content
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

    boolean stackHasType(List<Segment> stack, String type) {
        return stack.any { it.type == type }
    }

    List<Segment> parse(List<Token> tokens) {
        List<Segment> completed = []
        List<Segment> stack = [new Segment(type: 'SENTENCE')]

        tokens.each { tok ->
            if (stack.isEmpty()) stack.push(new Segment(type: 'SENTENCE'))
            Segment current = stack.last()

            switch (tok.type) {
                case 'WORD':
                    current.wordLengths << tok.wordLength
                    break

                case 'PAUSE':
                    current.children << new Segment(type: 'PAUSE', terminator: tok.value, childInsertPosition: current.wordLengths.size())
                    break

                case 'OPEN': // Parens
                    if (!stackHasType(stack, tok.context)) {
                        Segment child = new Segment(type: tok.context, childInsertPosition: current.wordLengths.size())
                        current.children << child
                        stack.push(child)
                    }
                    break

                case 'CLOSE': // Parens
                    if (current.type == tok.context) stack.removeLast()
                    break

                case 'OPEN_SQUOTE': // Single Quote Open
                    if (!stackHasType(stack, 'SQUOTE')) {
                        Segment child = new Segment(type: 'SQUOTE', childInsertPosition: current.wordLengths.size())
                        current.children << child
                        stack.push(child)
                    }
                    break

                case 'CLOSE_SQUOTE': // Single Quote Close
                    if (current.type == 'SQUOTE') {
                        stack.removeLast()
                    }
                    // Else: ignore per rules
                    break

                case 'TOGGLE': // Double Quotes, Dashes
                    if (current.type == tok.context) {
                        stack.removeLast()
                    } else if (!stackHasType(stack, tok.context)) {
                        Segment child = new Segment(type: tok.context, childInsertPosition: current.wordLengths.size())
                        current.children << child
                        stack.push(child)
                    }
                    break

                case 'TERM':
                    if (current.type == 'SENTENCE') {
                        current.terminator = tok.value
                        completed << stack.first()
                        stack.clear(); stack.push(new Segment(type: 'SENTENCE'))
                    }
                    else if (current.type == 'QUOTE' || current.type == 'DASH') {
                        current.terminator = tok.value
                        if (current.type == 'DASH') stack.removeLast()
                    }
                    else if (current.type == 'SQUOTE' || current.type == 'PAREN') {
                        stack.removeLast()
                        if (!stack.isEmpty()) {
                            stack.last().terminator = tok.value
                            if (stack.last().type == 'SENTENCE') {
                                completed << stack.first()
                                stack.clear(); stack.push(new Segment(type: 'SENTENCE'))
                            }
                        }
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
    boolean markingMode = false // New Flag
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
N-Gram Mode       : ${ngramMode} (Initial Target)
Unique Mode       : ${uniqueMode ? 'Active (Fallback: Tri->Bi->Uni)' : 'Inactive'}
Marking Mode      : ${markingMode ? 'Active (-m)' : 'Inactive'}
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
            case '-m': cfg.markingMode = true; break // Enable marking
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

    void process(Config cfg) {
        println "📖 Analyzing source..."
        def text = new File(cfg.f_File).getText("UTF-8")
        if (text.length() > 0 && text.charAt(0) == '\uFEFF') text = text.substring(1)

        text = text.replaceAll(/--+/, '\u2014') // Dashes
        text = text.replaceAll(/[“”]/, '"')      // Smart double quotes
        text = text.replaceAll(/[‘’]/, "'")      // Smart single quotes
        text = text.replaceAll(/\r?\n\s*\r?\n/, " . ") // Paragraphs
        text = text.replaceAll(/\r?\n/, " ") // Word wrap

        List<Token> tokens = tokenizer.tokenize(text)
        tokens = tokens.findAll { it != null }

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
            def segments = parser.parse(tokens)
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
    Map<String, Map<String, WeightedSelector>> markovSelectors = [:]
    Map<String, List<Integer>> lengthStats = [:]
    Map<String, List<String>> segmentStarts = [:]
    int contextDepth = 3

    void loadWordModel(Config cfg) {
        println "📂 Loading Word Model..."
        def json = new JsonSlurper().parse(new File(cfg.wordStatsFile))
        if (json.vocabulary) loadedVocabulary = new HashSet<>(json.vocabulary)
        mainLengthSelector = new WeightedSelector(json.lengthStartStats)
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
        json.transitions.each { k, v -> transitionSel[k] = new WeightedSelector(v) }
    }

    Segment generateStructure(String type, int depth) {
        Segment seg = new Segment(type: type)
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
            if (nextToken.startsWith("PAUSE") && context.size() > 0 && context.last().startsWith("PAUSE")) break
            if (currentLen >= targetLen * 1.5) {
                if (sel.keys.contains("END")) nextToken = "END"
                else if (nextToken.startsWith("PAUSE") || nextToken.startsWith("SEGMENT")) break
            }
            if (nextToken == "END") break
            applyToken(seg, nextToken, depth)
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

    // Formatting helper for Marking Mode
    String formatMarkedWord(String w, int startMode, int actualMode, boolean forced) {
        if (forced) return "[${w}]"

        if (startMode == 3) {
            if (actualMode == 2) return "<${w}>"
            if (actualMode == 1) return "{${w}}"
        } else if (startMode == 2) {
            if (actualMode == 1) return "{${w}}"
        }
        return w // No mark implies success at requested level
    }

    String renderSegment(Segment seg, Config cfg, Map stats) {
        def sb = new StringBuilder()

        // --- OPENERS ---
        if (seg.type == 'QUOTE') sb.append('"')
        else if (seg.type == 'SQUOTE') sb.append("'")
        else if (seg.type == 'PAREN') sb.append("(")
        else if (seg.type == 'DASH') sb.append("\u2014")

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

            // GENERATION & BACKOFF LOGIC
            int startMode = cfg.ngramMode
            int currentMode = startMode
            String w = generateWord(len, currentMode)
            boolean forced = false

            if (cfg.uniqueMode && loadedVocabulary) {
                int retries = 0
                while (loadedVocabulary.contains(w)) {
                    stats.filteredCount++
                    retries++
                    // Backoff logic
                    if (retries >= 20) {
                        if (startMode > 2) currentMode = 2
                    }
                    if (retries >= 40) {
                        if (startMode > 1) currentMode = 1
                    }
                    if (retries >= 60) {
                        stats.forcedCount++; forced = true; break
                    }
                    w = generateWord(len, currentMode)
                }
            }

            // Apply Marking if enabled
            if (cfg.markingMode) {
                w = formatMarkedWord(w, startMode, currentMode, forced)
            } else if (forced) {
                // If not marking mode, we usually output plain, but forced is technically an error/failure of uniqueness
                // Original behavior was usually to output [word] for forced only?
                // Previous logic always marked forced as [word]. Let's maintain that implied standard or use marking logic.
                // Reverting to standard output: just the word, unless marking mode is ON.
                // Wait, previous version did: sb.append(forced ? "[${w}]" : w)
                // So I will keep that default behavior if marking is OFF.
                w = "[${w}]"
            }

            // Capitalize first word of Quote or Sentence (ignore markers for cap check)
            if (i == 0 && (seg.type == 'SENTENCE' || seg.type == 'QUOTE')) w = Utils.smartCaps(w)

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

        // --- CLOSERS & TERMINATORS ---
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
            if (seg.terminator) sb.append(seg.terminator)
            else sb.append("\u2014")
        }
        else if (seg.type == 'SENTENCE') {
            if (seg.terminator) sb.append(seg.terminator)
        }

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
                root.terminator = transitionSel["TERM"]?.select(Globals.RND) ?: "."

                String output = renderSegment(root, cfg, stats)
                output = Utils.smartCaps(output)
                outStream.println(output)
                generatedCount++
            } else {
                def fused = mainLengthSelector.select(Globals.RND)
                if (!fused) break
                def targetLen = fused.split(':')[0].toInteger()
                String w = generateWord(targetLen, cfg.ngramMode)
                outStream.println(w); generatedCount++
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

// ----------------------------------------------------------------
// --- MAIN ---
// ----------------------------------------------------------------

def cfg = parseArgs(args)

if (!cfg.valid) {
    println "❌ Error: ${cfg.error}"
    println "Usage: groovy script.groovy -a -f input.txt -w words.json [-s sentences.json] OR -g ..."
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