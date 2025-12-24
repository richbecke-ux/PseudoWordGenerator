package org.tdl.ptg.split

import groovy.transform.Field
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
    static final Set<String> TERMINATORS = new HashSet(['.', '!', '?'])
    static final Set<String> ALL_PUNCT = new HashSet(['.', '!', '?', ',', ':', ';', '—', '-'])
    static final int MAX_EFF_LEN = 8
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
Input File     : ${f_File}
Word Stats Out : ${wordStatsFile}
Sent Stats Out : ${sentenceStatsFile ?: '(None)'}
Unique Mode    : ${uniqueMode ? 'Active (Saving vocabulary list)' : 'Inactive'}
=============================="""
        } else {
            return """
=== GENERATION CONFIGURATION ===
Word Stats In  : ${wordStatsFile}
Sent Stats In  : ${sentenceStatsFile ?: '(None) - Word List Mode'}
Output File    : ${f_File ?: 'STDOUT'}
Pruning        : ${pruneMinTokens > 0 ? "Min ${pruneMinTokens} tokens" : 'Disabled'}
N-Gram Mode    : ${ngramMode} (Initial Target)
Unique Mode    : ${uniqueMode ? 'Active (Fallback: Tri->Bi->Uni)' : 'Inactive'}
Count          : ${count}
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
        println "📖 Analyzing source..."

        def fileContent = new File(cfg.f_File).getText("UTF-8")
        if (fileContent.length() > 0 && fileContent.charAt(0) == '\uFEFF') fileContent = fileContent.substring(1)

        def text = fileContent.toLowerCase().replaceAll(/\r\n/, "\n").replaceAll(/\s+/, " ")
        def tokens = text.split(" ")

        def currentSegmentLen = 0
        def previousState = "START"
        List<Integer> currentStructure = []
        int wordsProcessed = 0

        tokens.each { token ->
            if (!token) return
            def lastChar = token.takeRight(1)
            def punctuation = null
            def wordPart = token
            if (Globals.ALL_PUNCT.contains(lastChar)) {
                punctuation = lastChar
                wordPart = token.dropRight(1)
            }

            def cleanPart = wordPart.findAll { Character.isLetter(it as char) }.join('')
            def hasVowel = cleanPart.any { Globals.VOWELS.contains(it.toString()) }

            if (cleanPart && hasVowel && cleanPart.length() <= 20) {
                def struct = Utils.tokenizeStructure(wordPart)
                if (struct) {
                    wordsProcessed++
                    if (cfg.uniqueMode) vocabulary.add(cleanPart)

                    def len = struct.size()
                    currentStructure.add(len)
                    def effLen = Math.min(len, Globals.MAX_EFF_LEN).toString()

                    def startType = struct[0][0]
                    def fusedKey = "${len}:${startType}"
                    lengthStartStats[fusedKey] = (lengthStartStats[fusedKey] ?: 0) + 1

                    struct.eachWithIndex { t, i ->
                        def content = t[1]
                        if (i == 0) {
                            Utils.ensureMap(startTokens, effLen, startType)[content] = (Utils.ensureMap(startTokens, effLen, startType)[content] ?: 0) + 1
                        } else {
                            def prev = struct[i-1]; def prevContent = prev[1]; def prevType = prev[0]
                            def baseMap = (i == len - 1) ? lastTokens : innerTokens
                            Utils.ensureMap(baseMap, effLen, prevType)[content] = (Utils.ensureMap(baseMap, effLen, prevType)[content] ?: 0) + 1

                            def bgMap = (i == len - 1) ? bigramLast : (i == 1) ? bigramStart : bigramInner
                            Utils.ensureMap(bgMap, effLen, prevContent)[content] = (Utils.ensureMap(bgMap, effLen, prevContent)[content] ?: 0) + 1

                            if (i >= 2) {
                                def prev2 = struct[i-2][1]
                                def tgKey = "${prev2}:${prevContent}"
                                def tgMap = (i == len - 1) ? trigramLast : trigramInner
                                Utils.ensureMap(tgMap, effLen, tgKey)[content] = (Utils.ensureMap(tgMap, effLen, tgKey)[content] ?: 0) + 1
                            }
                        }
                    }
                    currentSegmentLen++
                }
            }

            if (punctuation) {
                if (currentSegmentLen > 0) transitions.computeIfAbsent(previousState, {[:]}).merge(punctuation, 1, Integer::sum)
                if (currentStructure && cfg.sentenceStatsFile) segmentTemplates.add(new ArrayList(currentStructure))
                currentStructure.clear()
                currentSegmentLen = 0
                previousState = Globals.TERMINATORS.contains(punctuation) ? "START" : punctuation
            }
        }
        println "\n📊 Analysis Stats:"
        println "   Words Processed: ${wordsProcessed}"
        if (cfg.uniqueMode) println "   Unique Vocabulary: ${vocabulary.size()} words stored"
        println "   Length Templates: ${lengthStartStats.size()}"
        if (cfg.sentenceStatsFile) println "   Rhythm Templates: ${segmentTemplates.size()}"
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
    List<List<Integer>> templates = []
    Map<String, WeightedSelector> transitionSel = [:]

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
        this.templates = json.segmentTemplates
        json.transitions.each { k, v -> transitionSel[k] = new WeightedSelector(v) }
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
        int filteredCount = 0
        int forcedAcceptanceCount = 0

        // --- FALLBACK CONFIGURATION ---
        int limitTrigram = 20
        int limitBigram  = 40
        int limitUnigram = 60

        while (generatedCount < cfg.count) {

            if (cfg.sentenceStatsFile) {
                // --- SENTENCE MODE ---
                if (templates.isEmpty()) break
                def tpl = templates[Globals.RND.nextInt(templates.size())]
                def words = []

                for (int len : tpl) {
                    int currentMode = cfg.ngramMode
                    String w = generateWord(len, currentMode)
                    boolean forced = false

                    if (cfg.uniqueMode) {
                        int retries = 0
                        while (loadedVocabulary.contains(w)) {
                            filteredCount++
                            retries++

                            if (retries >= limitTrigram && retries < limitBigram) currentMode = 2
                            else if (retries >= limitBigram && retries < limitUnigram) currentMode = 1
                            else if (retries >= limitUnigram) {
                                forcedAcceptanceCount++
                                forced = true
                                break
                            }

                            w = generateWord(len, currentMode)
                        }
                    }
                    if (forced) words << "[${w}]"
                    else words << w
                }

                if (words) {
                    // Capitalize first word (even if bracketed)
                    // If bracketed: "[word]" -> "[Word]"
                    String first = words[0]
                    if (first.startsWith("[")) {
                        if (first.length() > 2) {
                            words[0] = "[" + first.substring(1, 2).toUpperCase() + first.substring(2)
                        }
                    } else {
                        words[0] = first.capitalize()
                    }
                }

                def p = (transitionSel["START"]?.select(Globals.RND)) ?: "."
                outStream.println(words.join(" ") + p)
                generatedCount++

            } else {
                // --- WORD LIST MODE ---
                def fused = mainLengthSelector.select(Globals.RND)
                if (!fused) break

                def targetLen = fused.split(':')[0].toInteger()
                int currentMode = cfg.ngramMode
                String w = generateWord(targetLen, currentMode)

                boolean accepted = true
                boolean forced = false

                if (cfg.uniqueMode) {
                    int retries = 0
                    while (loadedVocabulary.contains(w)) {
                        filteredCount++
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
            println "🔒 Filtered ${filteredCount} duplicates."
            if (forcedAcceptanceCount > 0) println "⚠️  Forced to accept ${forcedAcceptanceCount} real words (marked in [Brackets])."
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
    println """
PSEUDO FACTORY - USAGE:
  1. ANALYZE: groovy script.groovy -a -f <source.txt> -w <words.json> [-s <sent.json>] [-u]
  2. GENERATE: groovy script.groovy -g -w <words.json> [-s <sent.json>] [-c <n>] [-u] [-p <n>]
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