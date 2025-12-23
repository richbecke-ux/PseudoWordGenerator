package org.tdl.ptg.split

import groovy.transform.Field
import groovy.transform.Canonical
import groovy.json.JsonSlurper
import groovy.json.JsonOutput
import java.nio.file.Files
import java.nio.file.Paths

// ----------------------------------------------------------------
// --- CONFIGURATION ---
// ----------------------------------------------------------------

@Field final Random RND = new Random()
@Field final Set<String> VOWELS = new HashSet(['a', 'e', 'i', 'o', 'u', 'y', 'æ', 'ø', 'å', 'ä', 'ö', 'ü', 'é'])
@Field final Set<String> TERMINATORS = new HashSet(['.', '!', '?'])
@Field final Set<String> ALL_PUNCT = new HashSet(['.', '!', '?', ',', ':', ';', '—', '-'])
@Field final int MAX_EFF_LEN = 8

// ----------------------------------------------------------------
// --- CLI & CONFIG ---
// ----------------------------------------------------------------

@Canonical
class Config {
    // Mode Flags
    boolean analyze = false
    boolean generate = false

    // File Paths
    String wordStatsFile = null
    String sentenceStatsFile = null
    String f_File = null

    // Settings
    boolean uniqueMode = false
    int pruneMinTokens = 0          // Applies to GENERATION (Token count, not char length)
    int ngramMode = 3
    int count = 20

    boolean valid = false
    String error = ""
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

    // --- VALIDATION ---

    if (cfg.analyze && cfg.generate) {
        cfg.error = "Ambiguous Mode: Select either -a (Analyze) or -g (Generate)."
        return cfg
    }
    if (!cfg.analyze && !cfg.generate) {
        cfg.error = "No Mode Specified: Use -a (Analyze) or -g (Generate)."
        return cfg
    }

    if (cfg.analyze) {
        if (!cfg.f_File) { cfg.error = "Analysis Error: Source text (-f) required."; return cfg }
        if (!cfg.wordStatsFile) { cfg.error = "Analysis Error: Output file (-w) required."; return cfg }
    }

    if (cfg.generate) {
        if (!cfg.wordStatsFile) { cfg.error = "Generation Error: Input stats (-w) required."; return cfg }
    }

    cfg.valid = true
    return cfg
}

// ----------------------------------------------------------------
// --- UTILS ---
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

def tokenizeStructure(String word) {
    def cleanWord = word.findAll { Character.isLetter(it as char) }.join('').toLowerCase()
    if (!cleanWord) return []
    def tokens = []
    def curType = ''
    def curContent = new StringBuilder()
    cleanWord.each { c ->
        def type = VOWELS.contains(c.toString()) ? 'V' : 'C'
        if (curType == '' || curType == type) { curType = type; curContent.append(c) }
        else {
            tokens << [curType, curContent.toString()]
            curType = type
            curContent = new StringBuilder().append(c)
        }
    }
    if (curContent) tokens << [curType, curContent.toString()]
    return tokens
}

def ensureMap(Map map, String k1, String k2) {
    map.computeIfAbsent(k1, { [:] }).computeIfAbsent(k2, { [:] })
}

// ----------------------------------------------------------------
// --- ANALYZER ---
// ----------------------------------------------------------------

class Analyzer {
    Map<String, Integer> lengthStartStats = [:]

    Map<String, Map<String, Map<String, Integer>>> startTokens = [:]
    Map<String, Map<String, Map<String, Integer>>> innerTokens = [:]
    Map<String, Map<String, Map<String, Integer>>> lastTokens = [:]

    Map<String, Map<String, Map<String, Integer>>> bigramStart = [:]
    Map<String, Map<String, Map<String, Integer>>> bigramInner = [:]
    Map<String, Map<String, Map<String, Integer>>> bigramLast = [:]

    Map<String, Map<String, Map<String, Integer>>> trigramInner = [:]
    Map<String, Map<String, Map<String, Integer>>> trigramLast = [:]

    List<List<Integer>> segmentTemplates = []
    Map<String, Map<String, Integer>> transitions = [:]

    Set<String> vocabulary = new HashSet<>()

    void process(Config cfg) {
        println "📖 Analyzing source: ${cfg.f_File}..."
        def text = new File(cfg.f_File).getText("UTF-8").toLowerCase().replaceAll(/\r\n/, "\n").replaceAll(/\s+/, " ")
        def tokens = text.split(" ")

        def currentSegmentLen = 0
        def previousState = "START"
        List<Integer> currentStructure = []

        tokens.each { token ->
            if (!token) return
            def lastChar = token.takeRight(1)
            def punctuation = null
            def wordPart = token
            if (ALL_PUNCT.contains(lastChar)) {
                punctuation = lastChar
                wordPart = token.dropRight(1)
            }

            def cleanPart = wordPart.findAll { Character.isLetter(it as char) }.join('')
            def hasVowel = cleanPart.any { VOWELS.contains(it.toString()) }

            // Capture all valid words <= 20 tokens (pruning happens at generation)
            if (cleanPart && hasVowel && cleanPart.length() <= 20) {
                def struct = tokenizeStructure(wordPart)
                if (struct) {
                    if (cfg.uniqueMode) vocabulary.add(cleanPart)

                    def len = struct.size()
                    currentStructure.add(len)
                    def effLen = Math.min(len, MAX_EFF_LEN).toString()

                    def startType = struct[0][0]
                    def fusedKey = "${len}:${startType}"
                    lengthStartStats[fusedKey] = (lengthStartStats[fusedKey] ?: 0) + 1

                    struct.eachWithIndex { t, i ->
                        def content = t[1]
                        if (i == 0) {
                            ensureMap(startTokens, effLen, startType)[content] = (ensureMap(startTokens, effLen, startType)[content] ?: 0) + 1
                        } else {
                            def prev = struct[i-1]; def prevContent = prev[1]; def prevType = prev[0]
                            def baseMap = (i == len - 1) ? lastTokens : innerTokens
                            ensureMap(baseMap, effLen, prevType)[content] = (ensureMap(baseMap, effLen, prevType)[content] ?: 0) + 1

                            def bgMap = (i == len - 1) ? bigramLast : (i == 1) ? bigramStart : bigramInner
                            ensureMap(bgMap, effLen, prevContent)[content] = (ensureMap(bgMap, effLen, prevContent)[content] ?: 0) + 1

                            if (i >= 2) {
                                def prev2 = struct[i-2][1]
                                def tgKey = "${prev2}:${prevContent}"
                                def tgMap = (i == len - 1) ? trigramLast : trigramInner
                                ensureMap(tgMap, effLen, tgKey)[content] = (ensureMap(tgMap, effLen, tgKey)[content] ?: 0) + 1
                            }
                        }
                    }
                    currentSegmentLen++
                }
            }

            if (punctuation) {
                if (currentSegmentLen > 0) {
                    transitions.computeIfAbsent(previousState, {[:]}).merge(punctuation, 1, Integer::sum)
                }
                if (currentStructure && cfg.sentenceStatsFile) {
                    segmentTemplates.add(new ArrayList(currentStructure))
                }
                currentStructure.clear()
                currentSegmentLen = 0
                previousState = TERMINATORS.contains(punctuation) ? "START" : punctuation
            }
        }
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

    // Selectors
    Map<String, Map<String, WeightedSelector>> startSel = [:]
    Map<String, Map<String, WeightedSelector>> innerSel = [:]
    Map<String, Map<String, WeightedSelector>> lastSel = [:]
    Map<String, Map<String, WeightedSelector>> bigramStartSel = [:]
    Map<String, Map<String, WeightedSelector>> bigramInnerSel = [:]
    Map<String, Map<String, WeightedSelector>> bigramLastSel = [:]
    Map<String, Map<String, WeightedSelector>> trigramInnerSel = [:]
    Map<String, Map<String, WeightedSelector>> trigramLastSel = [:]

    Set<String> loadedVocabulary = null
    List<List<Integer>> templates = []
    Map<String, WeightedSelector> transitionSel = [:]

    void loadWordModel(Config cfg) {
        println "📂 Loading Word Model: ${cfg.wordStatsFile}..."
        def json = new JsonSlurper().parse(new File(cfg.wordStatsFile))

        if (json.vocabulary) loadedVocabulary = new HashSet<>(json.vocabulary)

        // --- PRUNING LOGIC ---
        Map<String, Integer> rawLengthStats = json.lengthStartStats
        if (cfg.pruneMinTokens > 0) {
            println "✂️  Pruning Enabled: Filtering words with <= ${cfg.pruneMinTokens} tokens."
            rawLengthStats = rawLengthStats.findAll { k, v ->
                def len = k.split(':')[0].toInteger()
                return len > cfg.pruneMinTokens
            }
            if (rawLengthStats.isEmpty()) {
                println "❌ Error: Pruning removed ALL word templates. Decrease -p value."
                System.exit(1)
            }
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
        println "📂 Loading Rhythm Model: ${path}..."
        def json = new JsonSlurper().parse(new File(path))
        this.templates = json.segmentTemplates
        json.transitions.each { k, v -> transitionSel[k] = new WeightedSelector(v) }
    }

    String generateWord(int targetLen, int ngramMode) {
        def lenKey = targetLen.toString()
        def typeSel = lengthToStartTypeSel[lenKey]

        if (!typeSel) {
            def fused = mainLengthSelector.select(RND)
            if (!fused) return "blob"
            lenKey = fused.split(':')[0]
            targetLen = lenKey.toInteger()
            typeSel = new WeightedSelector([(fused.split(':')[1]): 1])
        }

        def startType = typeSel.select(RND)
        def effLen = Math.min(targetLen, MAX_EFF_LEN).toString()

        def tokens = []
        def startContent = startSel[effLen]?[startType]?.select(RND) ?: "err"
        tokens << startContent
        def prevType = startType

        for (int i = 1; i < targetLen; i++) {
            def isLast = (i == targetLen - 1)
            def token = null

            // Trigram
            if (ngramMode >= 3 && tokens.size() >= 2) {
                def tgKey = "${tokens[-2]}:${tokens[-1]}"
                def sel = isLast ? trigramLastSel : trigramInnerSel
                token = sel[effLen]?[tgKey]?.select(RND)
            }
            // Bigram
            if (!token && ngramMode >= 2) {
                def bgKey = tokens[-1]
                def sel = isLast ? bigramLastSel : (i == 1 ? bigramStartSel : bigramInnerSel)
                token = sel[effLen]?[bgKey]?.select(RND)
            }
            // Unigram
            if (!token) {
                def sel = isLast ? lastSel : innerSel
                token = sel[effLen]?[prevType]?.select(RND)
            }

            if (!token) token = (prevType == 'C') ? 'a' : 'b'
            tokens << token
            prevType = (prevType == 'C' ? 'V' : 'C')
        }
        return tokens.join('')
    }

    void execute(Config cfg) {
        if (cfg.uniqueMode && !loadedVocabulary) {
            println "❌ Error: Unique Mode (-u) requires vocabulary list. Run analysis with -u first."
            System.exit(1)
        }

        PrintStream outStream = System.out
        if (cfg.f_File) {
            outStream = new PrintStream(new File(cfg.f_File))
            println "✍️  Output file: ${cfg.f_File}"
        }

        int attempts = 0
        int generatedCount = 0

        while (generatedCount < cfg.count) {

            if (cfg.sentenceStatsFile) {
                if (templates.isEmpty()) break
                def tpl = templates[RND.nextInt(templates.size())]

                def words = []
                tpl.each { len -> words << generateWord(len, cfg.ngramMode) }
                if (words) words[0] = words[0].capitalize()

                def p = (transitionSel["START"]?.select(RND)) ?: "."
                outStream.println(words.join(" ") + p)
                generatedCount++

            } else {
                def fused = mainLengthSelector.select(RND)
                if (!fused) break

                def targetLen = fused.split(':')[0].toInteger()
                def w = generateWord(targetLen, cfg.ngramMode)

                if (cfg.uniqueMode && loadedVocabulary.contains(w)) {
                    attempts++
                    if (attempts > 500) break
                    continue
                }

                outStream.println(w)
                generatedCount++
                attempts = 0
            }
        }

        if (cfg.f_File) outStream.close()
        else println "Done."
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

  1. ANALYSIS (-a)
     groovy PseudoFactory.groovy -a -f <source.txt> -w <words.json> [-s <sent.json>] [-u]

  2. GENERATION (-g)
     groovy PseudoFactory.groovy -g -w <words.json> [-s <sent.json>] [options]

     Options:
       -f <file> : Output text file (default stdout)
       -p <n>    : Prune words <= n TOKENS (Word Mode only)
       -c <n>    : Count items to generate
       -u        : Unique mode (requires -u in analysis)
       -1|-2|-3  : N-gram depth (default 3)
"""
    System.exit(1)
}

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