package org.tdl.ptg

import groovy.transform.Canonical
import java.nio.file.Files
import java.nio.file.Paths

// ----------------------------------------------------------------
// --- SHARED CONSTANTS ---
// ----------------------------------------------------------------

class Config {
    static final Random RND = new Random()
    static final Set<String> VOWELS = new HashSet(['a', 'e', 'i', 'o', 'u', 'y', 'æ', 'ø', 'å', 'ä', 'ö', 'ü', 'é'])
    static final Set<String> TERMINATORS = new HashSet(['.', '!', '?'])
    static final Set<String> INTRA_PUNCT = new HashSet([',', ':', ';', '—', '-'])
    static final Set<String> ALL_PUNCT = TERMINATORS + INTRA_PUNCT

    // N-gram modes
    static final int NGRAM_NONE = 0
    static final int NGRAM_BIGRAM = 1
    static final int NGRAM_TRIGRAM = 2

    static final int MAX_EFF_LEN = 8

    // Probability (0.0 - 1.0) to ignore a valid Trigram and force a Bigram choice.
    static final double CHAOS_FACTOR = 0.15

    // Max attempts to generate a unique word before giving up
    static final int MAX_UNIQUE_RETRIES = 50
}

// ----------------------------------------------------------------
// --- DATA STRUCTURES ---
// ----------------------------------------------------------------

@Canonical
class WeightedSelector {
    final List<String> keys
    final List<Long> cumulativeWeights
    final long totalWeight

    WeightedSelector(Map<String, Integer> map) {
        this.keys = new ArrayList<>()
        this.cumulativeWeights = new ArrayList<>()
        long runningTotal = 0L

        // Sorting helps binary search slightly
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
        if (totalWeight <= 0 || keys.isEmpty()) return null
        long target = (long) (rnd.nextDouble() * totalWeight) + 1
        int index = Collections.binarySearch(cumulativeWeights, target)
        if (index < 0) index = -index - 1
        if (index >= keys.size()) index = keys.size() - 1
        return keys[index]
    }
}

@Canonical
class CommandLineArgs {
    String filePath = null
    String mode = null
    int count = 0
    int ngramMode = Config.NGRAM_TRIGRAM // Default to Trigram
    boolean uniqueMode = true
    boolean valid = false
    String errorMessage = null
}

class Model {
    // Stats
    Map<String, Integer> lengthStartStats = [:]

    // The "Memory" of the corpus (used to reject duplicates)
    Set<String> vocabulary = new HashSet<>()

    // Hierarchy: [EffectiveLength][PreviousType][Token] -> Count
    Map<Integer, Map<String, Map<String, Integer>>> startTokens = [:]
    Map<Integer, Map<String, Map<String, Integer>>> innerTokens = [:]
    Map<Integer, Map<String, Map<String, Integer>>> lastTokens = [:]

    // N-Grams
    Map<Integer, Map<String, Map<String, Integer>>> bigramStart = [:]
    Map<Integer, Map<String, Map<String, Integer>>> bigramInner = [:]
    Map<Integer, Map<String, Map<String, Integer>>> bigramLast = [:]
    Map<Integer, Map<String, Map<String, Integer>>> trigramInner = [:]
    Map<Integer, Map<String, Map<String, Integer>>> trigramLast = [:]

    // Sentence Rhythm (Direct Distribution)
    List<Integer> sentenceLengthHistory = []
    Map<String, Map<String, Integer>> transitions = [:]

    int ngramMode = 0

    // Selectors
    WeightedSelector lengthStartSelector
    Map<Integer, Map<String, WeightedSelector>> startSelectors = [:]
    Map<Integer, Map<String, WeightedSelector>> innerSelectors = [:]
    Map<Integer, Map<String, WeightedSelector>> lastSelectors = [:]
    Map<Integer, Map<String, WeightedSelector>> bigramStartSelectors = [:]
    Map<Integer, Map<String, WeightedSelector>> bigramInnerSelectors = [:]
    Map<Integer, Map<String, WeightedSelector>> bigramLastSelectors = [:]
    Map<Integer, Map<String, WeightedSelector>> trigramInnerSelectors = [:]
    Map<Integer, Map<String, WeightedSelector>> trigramLastSelectors = [:]
    Map<String, WeightedSelector> transitionSelectors = [:]
}

// ----------------------------------------------------------------
// --- LOGIC ---
// ----------------------------------------------------------------

def parseCommandLine(String[] args) {
    def result = new CommandLineArgs()
    def i = 0
    while (i < args.length) {
        def arg = args[i]
        switch (arg) {
            case '-f': result.filePath = args[++i]; break
            case '-s': result.mode = '-s'; result.count = args[++i].toInteger(); break
            case '-w': result.mode = '-w'; result.count = args[++i].toInteger(); break
            case '-b': result.ngramMode = Config.NGRAM_BIGRAM; break
            case '-t': result.ngramMode = Config.NGRAM_TRIGRAM; break
            case '-u': result.uniqueMode = true; break
            case '-nu': result.uniqueMode = false; break
        }
        i++
    }
    if (result.filePath && result.mode && result.count > 0) result.valid = true
    else result.errorMessage = "Missing arguments. Need -f, and -s or -w."
    return result
}

def getEffectiveLength(int len) { Math.min(len, Config.MAX_EFF_LEN) }

def tokenizeWordStructure(String word) {
    def cleanWord = word.findAll { Character.isLetter(it as char) }.join('').toLowerCase()
    if (!cleanWord) return []

    def tokens = []
    def currentType = ''
    def currentContent = new StringBuilder()

    cleanWord.each { c ->
        def charStr = c.toString()
        def isVowel = Config.VOWELS.contains(charStr)
        def charType = isVowel ? 'V' : 'C'

        if (currentType == '') {
            currentType = charType
            currentContent.append(charStr)
        } else if (charType == currentType) {
            currentContent.append(charStr)
        } else {
            tokens << [currentType, currentContent.toString()]
            currentType = charType
            currentContent = new StringBuilder().append(charStr)
        }
    }
    if (currentContent.length() > 0) tokens << [currentType, currentContent.toString()]
    return tokens
}

def ensureNestedMap(Map map, int k1, String k2) {
    map.computeIfAbsent(k1, { [:] }).computeIfAbsent(k2, { [:] })
}

def analyzeText(String filePath, int ngramMode) {
    def text = Files.readString(Paths.get(filePath)).toLowerCase()
    text = text.replaceAll(/\r\n/, "\n").replaceAll(/\s+/, " ")

    def rawTokens = text.split(" ")
    def model = new Model()
    model.ngramMode = ngramMode

    def currentSegmentLen = 0
    def wordsInCurrentSentence = 0
    def previousState = "START"

    rawTokens.each { token ->
        if (!token) return

        def lastChar = token.takeRight(1)
        def punctuation = null
        def wordPart = token

        if (Config.ALL_PUNCT.contains(lastChar)) {
            punctuation = lastChar
            wordPart = token.dropRight(1)
        }

        // --- Word Analysis ---
        def structureTokens = tokenizeWordStructure(wordPart)
        if (structureTokens) {
            model.vocabulary.add(wordPart)

            def len = structureTokens.size()
            def effLen = getEffectiveLength(len)
            def firstToken = structureTokens[0]
            def startType = firstToken[0]

            // 1. Length & Start Type Stats
            def fusedKey = "${len}:${startType}"
            model.lengthStartStats[fusedKey] = (model.lengthStartStats[fusedKey] ?: 0) + 1

            // 2. Token Content Stats
            structureTokens.eachWithIndex { t, i ->
                def content = t[1]
                def type = t[0]

                if (i == 0) {
                    ensureNestedMap(model.startTokens, effLen, startType)[content] =
                            (ensureNestedMap(model.startTokens, effLen, startType)[content] ?: 0) + 1
                } else {
                    def prevToken = structureTokens[i - 1]
                    def prevContent = prevToken[1]
                    def prevType = prevToken[0]

                    // Simple Markov (Type -> Content)
                    def baseMap = (i == len - 1) ? model.lastTokens : model.innerTokens
                    ensureNestedMap(baseMap, effLen, prevType)[content] =
                            (ensureNestedMap(baseMap, effLen, prevType)[content] ?: 0) + 1

                    // Bigram (PrevContent -> Content)
                    def bgMap = (i == len - 1) ? model.bigramLast :
                            (i == 1) ? model.bigramStart : model.bigramInner
                    ensureNestedMap(bgMap, effLen, prevContent)[content] =
                            (ensureNestedMap(bgMap, effLen, prevContent)[content] ?: 0) + 1

                    // Trigram (Prev2 + Prev1 -> Content)
                    if (i >= 2) {
                        def prev2 = structureTokens[i - 2][1]
                        def tgKey = "${prev2}:${prevContent}"
                        def tgMap = (i == len - 1) ? model.trigramLast : model.trigramInner
                        ensureNestedMap(tgMap, effLen, tgKey)[content] =
                                (ensureNestedMap(tgMap, effLen, tgKey)[content] ?: 0) + 1
                    }
                }
            }
            currentSegmentLen++
            wordsInCurrentSentence++
        }

        // --- Sentence Analysis ---
        if (punctuation) {
            if (currentSegmentLen > 0) {
                // Transition: State -> Punctuation
                def transMap = model.transitions.computeIfAbsent(previousState, { [:] })
                transMap[punctuation] = (transMap[punctuation] ?: 0) + 1
            }

            currentSegmentLen = 0
            if (Config.TERMINATORS.contains(punctuation)) {
                if (wordsInCurrentSentence > 0) {
                    model.sentenceLengthHistory.add(wordsInCurrentSentence)
                }
                wordsInCurrentSentence = 0
                previousState = "START"
            } else {
                previousState = punctuation
            }
        }
    }
    return model
}

def buildModelSelectors(Model model) {
    model.lengthStartSelector = new WeightedSelector(model.lengthStartStats)

    // Helper closure
    def build = { src, dest ->
        src.each { k1, inner -> dest[k1] = [:]
            inner.each { k2, map -> dest[k1][k2] = new WeightedSelector(map) }
        }
    }

    build(model.startTokens, model.startSelectors)
    build(model.innerTokens, model.innerSelectors)
    build(model.lastTokens, model.lastSelectors)
    build(model.bigramStart, model.bigramStartSelectors)
    build(model.bigramInner, model.bigramInnerSelectors)
    build(model.bigramLast, model.bigramLastSelectors)
    build(model.trigramInner, model.trigramInnerSelectors)
    build(model.trigramLast, model.trigramLastSelectors)

    model.transitions.each { k, v -> model.transitionSelectors[k] = new WeightedSelector(v) }
}

// ----------------------------------------------------------------
// --- GENERATION ---
// ----------------------------------------------------------------

def generateRawWord(Model model) {
    def fusedKey = model.lengthStartSelector.select(Config.RND)
    if (!fusedKey) return "blob"

    def parts = fusedKey.split(':')
    def len = parts[0].toInteger()
    def startType = parts[1]
    def effLen = getEffectiveLength(len)

    def tokens = []

    // 1. Start
    def startContent = model.startSelectors[effLen]?[startType]?.select(Config.RND)
    if (!startContent) return "start_err"
    tokens << startContent
    def prevType = startType

    // 2. Build
    for (int i = 1; i < len; i++) {
        def isLast = (i == len - 1)
        def token = null

        // A. Trigram Attempt (with Chaos Factor backoff)
        if (model.ngramMode >= Config.NGRAM_TRIGRAM && tokens.size() >= 2) {
            // Roll dice for Chaos: if successful, skip Trigram to force novelty
            if (Config.RND.nextDouble() > Config.CHAOS_FACTOR) {
                def tgKey = "${tokens[-2]}:${tokens[-1]}"
                def sel = isLast ? model.trigramLastSelectors : model.trigramInnerSelectors
                token = sel[effLen]?[tgKey]?.select(Config.RND)
            }
        }

        // B. Bigram Attempt
        if (!token && model.ngramMode >= Config.NGRAM_BIGRAM) {
            def bgKey = tokens[-1]
            def sel
            if (isLast) sel = model.bigramLastSelectors
            else if (i == 1) sel = model.bigramStartSelectors
            else sel = model.bigramInnerSelectors
            token = sel[effLen]?[bgKey]?.select(Config.RND)
        }

        // C. Fallback (Type-based)
        if (!token) {
            def sel = isLast ? model.lastSelectors : model.innerSelectors
            token = sel[effLen]?[prevType]?.select(Config.RND)
        }

        if (!token) token = (prevType == 'C') ? 'a' : 'b' // Emergency fallback
        tokens << token
        prevType = (prevType == 'C' ? 'V' : 'C')
    }

    return tokens.join('')
}

def generateWord(Model model, boolean uniqueMode) {
    int attempts = 0
    String word
    while (attempts < Config.MAX_UNIQUE_RETRIES) {
        word = generateRawWord(model)
        // If we want unique words, retry if the word is in the input vocabulary
        if (uniqueMode && model.vocabulary.contains(word)) {
            attempts++
            continue
        }
        return word
    }
    return word // Give up and return duplicate
}

def generateSentences(Model model, int count, boolean uniqueMode) {
    def sb = new StringBuilder()
    def currentState = "START"
    def historySize = model.sentenceLengthHistory.size()

    count.times {
        // 1. Determine Length by direct sampling (preserves rhythm)
        int targetWords = 10 // Default
        if (historySize > 0) {
            targetWords = model.sentenceLengthHistory[Config.RND.nextInt(historySize)]
        }

        // 2. Generate Words
        def words = []
        targetWords.times { words << generateWord(model, uniqueMode) }

        if (words) words[0] = words[0].capitalize()

        // 3. Append to block
        sb.append(words.join(" "))

        // 4. Determine Punctuation
        def punctSel = model.transitionSelectors[currentState]
        if (!punctSel) punctSel = model.transitionSelectors["START"] // Fallback

        def punct = punctSel ? punctSel.select(Config.RND) : "."
        sb.append(punct).append(" ")

        if (Config.TERMINATORS.contains(punct)) currentState = "START"
        else currentState = punct
    }

    println sb.toString().trim().replaceAll("(.{1,80})\\s+", "\$1\n")
}

// ----------------------------------------------------------------
// --- MAIN ---
// ----------------------------------------------------------------

def argsObj = parseCommandLine(args)
if (!argsObj.valid) {
    println "Error: ${argsObj.errorMessage}"
    println "Usage: groovy script.groovy -f <file> -s <sentences> [-w <words>] [-t|-b] [-u]"
    System.exit(1)
}

println "📖 Analyzing ${argsObj.filePath} (Trigram + Chaos Backoff)..."
def model = analyzeText(argsObj.filePath, argsObj.ngramMode)
buildModelSelectors(model)

println "📊 Stats: ${model.vocabulary.size()} unique words found."
println "🎲 Chaos Factor: ${Config.CHAOS_FACTOR * 100}% (Mixing bigrams into trigrams)"
if (argsObj.uniqueMode) println "✨ Unique Mode: Active (Filtering exact dictionary matches)"

println "\n✍️  OUTPUT:"
println "=" * 60

if (argsObj.mode == '-w') {
    argsObj.count.times { println generateWord(model, argsObj.uniqueMode) }
} else {
    generateSentences(model, argsObj.count, argsObj.uniqueMode)
}
println "=" * 60