package org.tdl.pwg.gemini

import groovy.transform.Field
import groovy.transform.Canonical
import java.nio.file.Files
import java.nio.file.Paths

// --- Configuration ---

@Field final Random RND = new Random()
@Field final Set<String> VOWELS = new HashSet(['a', 'e', 'i', 'o', 'u', 'y', 'æ', 'ø', 'å', 'ä', 'ö', 'ü', 'é'])
@Field final Set<String> TERMINATORS = new HashSet(['.', '!', '?'])
@Field final Set<String> INTRA_PUNCT = new HashSet([',', ':', ';'])
@Field final Set<String> ALL_PUNCT = TERMINATORS + INTRA_PUNCT

@Field final int MAX_EFF_LEN = 8

// N-gram modes
@Field final int NGRAM_NONE = 0
@Field final int NGRAM_BIGRAM = 1
@Field final int NGRAM_TRIGRAM = 2


// ----------------------------------------------------------------
// --- DATA STRUCTURES ---
// ----------------------------------------------------------------

/**
 * Implements O(log N) weighted random selection using cumulative weights and binary search.
 * Pre-computes cumulative distribution for fast repeated sampling.
 */
@Canonical
class WeightedSelector {
    final List<String> keys
    final List<Long> cumulativeWeights
    final long totalWeight

    WeightedSelector(Map<String, Integer> map) {
        this.keys = new ArrayList<>()
        this.cumulativeWeights = new ArrayList<>()
        long runningTotal = 0L

        map.each { key, weight ->
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

        // Generate random value in range [1, totalWeight]
        long target = (long) (rnd.nextDouble() * totalWeight) + 1

        // Binary search for first cumulative weight >= target
        int index = Collections.binarySearch(cumulativeWeights, target)

        if (index < 0) {
            // binarySearch returns (-(insertion point) - 1) when not found
            index = -index - 1
        }

        // Clamp to valid range
        if (index >= keys.size()) {
            index = keys.size() - 1
        }

        return keys[index]
    }

    boolean isEmpty() {
        return totalWeight <= 0 || keys.isEmpty()
    }
}

/**
 * Command line arguments container.
 */
@Canonical
class CommandLineArgs {
    String filePath = null
    String mode = null      // "-s" or "-w"
    int count = 0
    int ngramMode = 0       // NGRAM_NONE, NGRAM_BIGRAM, or NGRAM_TRIGRAM
    int pruneMinTokens = 0  // Minimum token count (0 = no pruning)
    boolean valid = false
    String errorMessage = null
}

/**
 * Data model storing all corpus statistics.
 * Raw count maps are populated during analysis, then selectors are built for O(log N) generation.
 */
class Model {
    // Raw count maps (populated during analysis)
    Map<String, Integer> lengthStartStats = [:]
    Map<Integer, Map<String, Map<String, Integer>>> startTokens = [:]
    Map<Integer, Map<String, Map<String, Integer>>> innerTokens = [:]
    Map<Integer, Map<String, Map<String, Integer>>> lastTokens = [:]
    Map<Integer, Map<String, Map<String, Integer>>> bigramStart = [:]
    Map<Integer, Map<String, Map<String, Integer>>> bigramInner = [:]
    Map<Integer, Map<String, Map<String, Integer>>> bigramLast = [:]
    Map<Integer, Map<String, Map<String, Integer>>> trigramInner = [:]
    Map<Integer, Map<String, Map<String, Integer>>> trigramLast = [:]

    // Sentence structure
    Map<String, List<Integer>> segmentLengths = [:]
    Map<String, Map<String, Integer>> transitions = [:]
    List<Integer> sentenceLengths = []

    int ngramMode = 0  // 0=none, 1=bigram, 2=trigram

    // Pre-built weighted selectors (populated after analysis for O(log N) lookups)
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

class ModelValidationException extends RuntimeException {
    ModelValidationException(String message) { super(message) }
}

class GenerationException extends RuntimeException {
    GenerationException(String message) { super(message) }
}

// ----------------------------------------------------------------
// --- COMMAND LINE PARSING ---
// ----------------------------------------------------------------

def parseCommandLine(String[] args) {
    def result = new CommandLineArgs()
    def i = 0

    while (i < args.length) {
        def arg = args[i]

        switch (arg) {
            case '-f':
                if (i + 1 >= args.length) {
                    result.errorMessage = "Option -f requires a filename argument"
                    return result
                }
                result.filePath = args[++i]
                break

            case '-s':
            case '-w':
                if (i + 1 >= args.length) {
                    result.errorMessage = "Option ${arg} requires a number argument"
                    return result
                }
                if (result.mode != null) {
                    result.errorMessage = "Cannot specify both -s and -w"
                    return result
                }
                result.mode = arg
                try {
                    result.count = args[++i].toInteger()
                } catch (NumberFormatException e) {
                    result.errorMessage = "Invalid number for ${arg}: ${args[i]}"
                    return result
                }
                break

            case '-b':
                if (result.ngramMode != NGRAM_NONE) {
                    result.errorMessage = "Cannot specify both -b and -t"
                    return result
                }
                result.ngramMode = NGRAM_BIGRAM
                break

            case '-t':
                if (result.ngramMode != NGRAM_NONE) {
                    result.errorMessage = "Cannot specify both -b and -t"
                    return result
                }
                result.ngramMode = NGRAM_TRIGRAM
                break

            case '-p':
                if (i + 1 >= args.length) {
                    result.errorMessage = "Option -p requires a number argument"
                    return result
                }
                try {
                    result.pruneMinTokens = args[++i].toInteger()
                } catch (NumberFormatException e) {
                    result.errorMessage = "Invalid number for -p: ${args[i]}"
                    return result
                }
                if (result.pruneMinTokens < 1) {
                    result.errorMessage = "Prune value must be at least 1"
                    return result
                }
                break

            default:
                result.errorMessage = "Unknown option: ${arg}"
                return result
        }
        i++
    }

    if (result.filePath == null) {
        result.errorMessage = "Missing required option: -f <filename>"
        return result
    }
    if (result.mode == null) {
        result.errorMessage = "Missing required option: -s <sentences> or -w <words>"
        return result
    }
    if (result.count <= 0) {
        result.errorMessage = "Count must be a positive integer"
        return result
    }

    // Prune option only allowed in word mode
    if (result.pruneMinTokens > 0 && result.mode != '-w') {
        result.errorMessage = "Option -p (prune) is only allowed in word mode (-w)"
        return result
    }

    result.valid = true
    return result
}

// ----------------------------------------------------------------
// --- HELPER FUNCTIONS ---
// ----------------------------------------------------------------

def getEffectiveLength(int len) {
    Math.min(len, MAX_EFF_LEN)
}

def getSegmentLengthBucket(int segLen) {
    if (segLen <= 0) return "1"
    if (segLen == 1) return "1"
    if (segLen == 2) return "2"
    if (segLen == 3) return "3"
    if (segLen <= 5) return "4-5"
    if (segLen <= 8) return "6-8"
    return "9+"
}

/**
 * Tokenizes a word into alternating consonant/vowel clusters.
 * Uses Character.isLetter() for robust Unicode support.
 * Returns list of [type, content] tuples.
 */
def tokenizeWordStructure(String word) {
    // Filter to letters only (handles all Unicode letters)
    def cleanWord = word.findAll { Character.isLetter(it as char) }.join('').toLowerCase()
    if (!cleanWord) return []

    def tokens = []
    def currentType = ''
    def currentContent = new StringBuilder()

    cleanWord.each { c ->
        def charStr = c.toString()
        def isVowel = VOWELS.contains(charStr)
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
    if (currentContent.length() > 0) {
        tokens << [currentType, currentContent.toString()]
    }
    return tokens
}

// ----------------------------------------------------------------
// --- PROACTIVE MAP ACCESS (ensures structure exists) ---
// ----------------------------------------------------------------

def ensureNestedMap(Map<Integer, Map<String, Map<String, Integer>>> parent, int key1, String key2) {
    if (!parent.containsKey(key1)) {
        parent[key1] = new HashMap<String, Map<String, Integer>>()
    }
    if (!parent[key1].containsKey(key2)) {
        parent[key1][key2] = new HashMap<String, Integer>()
    }
    return parent[key1][key2]
}

def ensureTransitionMap(Model model, String key) {
    if (!model.transitions.containsKey(key)) {
        model.transitions[key] = new HashMap<String, Integer>()
    }
    return model.transitions[key]
}

def ensureSegmentList(Model model, String state) {
    if (!model.segmentLengths.containsKey(state)) {
        model.segmentLengths[state] = new ArrayList<Integer>()
    }
    return model.segmentLengths[state]
}

// ----------------------------------------------------------------
// --- MODEL PRUNING (removes short words from generation) ---
// ----------------------------------------------------------------

def pruneModel(Model model, int minTokens) {
    if (minTokens <= 0) return  // No pruning needed

    // Find keys to remove (words with minTokens or fewer tokens)
    def keysToRemove = model.lengthStartStats.keySet().findAll { fusedKey ->
        def len = fusedKey.split(':')[0].toInteger()
        len <= minTokens
    }

    // Check if pruning would remove all keys
    if (keysToRemove.size() >= model.lengthStartStats.size()) {
        throw new ModelValidationException(
                "Pruning with -p ${minTokens} would remove all words. " +
                        "The corpus has no words with more than ${minTokens} tokens."
        )
    }

    // Remove the keys
    keysToRemove.each { key ->
        model.lengthStartStats.remove(key)
    }

    def removedCount = keysToRemove.size()
    def remainingCount = model.lengthStartStats.size()
    println "🔧 Pruned ${removedCount} word length categories (tokens ≤ ${minTokens}), ${remainingCount} remaining"
}

// ----------------------------------------------------------------
// --- SELECTOR BUILDING (converts count maps to O(log N) selectors) ---
// ----------------------------------------------------------------

def buildModelSelectors(Model model) {
    // Word length/type selector
    model.lengthStartSelector = new WeightedSelector(model.lengthStartStats)

    // Helper to build nested selectors from nested count maps
    def buildNestedSelectors = { Map<Integer, Map<String, Map<String, Integer>>> sourceMap,
                                 Map<Integer, Map<String, WeightedSelector>> selectorMap ->
        sourceMap.each { effLen, innerMap ->
            selectorMap[effLen] = new HashMap<String, WeightedSelector>()
            innerMap.each { key, weightedMap ->
                selectorMap[effLen][key] = new WeightedSelector(weightedMap)
            }
        }
    }

    // Token selectors
    buildNestedSelectors(model.startTokens, model.startSelectors)
    buildNestedSelectors(model.innerTokens, model.innerSelectors)
    buildNestedSelectors(model.lastTokens, model.lastSelectors)

    // N-gram selectors
    buildNestedSelectors(model.bigramStart, model.bigramStartSelectors)
    buildNestedSelectors(model.bigramInner, model.bigramInnerSelectors)
    buildNestedSelectors(model.bigramLast, model.bigramLastSelectors)
    buildNestedSelectors(model.trigramInner, model.trigramInnerSelectors)
    buildNestedSelectors(model.trigramLast, model.trigramLastSelectors)

    // Transition selectors
    model.transitions.each { key, weightedMap ->
        model.transitionSelectors[key] = new WeightedSelector(weightedMap)
    }
}

// ----------------------------------------------------------------
// --- ANALYSIS ---
// ----------------------------------------------------------------

def analyzeText(String filePath, int ngramMode) {
    def text = Files.readString(Paths.get(filePath)).toLowerCase()
    def rawTokens = text.split(/\s+/)
    def model = new Model()
    model.ngramMode = ngramMode

    def currentSegmentLen = 0
    def wordsInCurrentSentence = 0
    def previousState = "START"
    def previousPunctuation = null

    rawTokens.each { token ->
        if (!token) return

        def lastChar = token.takeRight(1)
        def punctuation = null
        def wordPart = token

        if (ALL_PUNCT.contains(lastChar)) {
            punctuation = lastChar
            wordPart = token.dropRight(1)
        }

        // --- 1. Analyze Word Structure ---
        def structureTokens = tokenizeWordStructure(wordPart)
        if (structureTokens) {
            def len = structureTokens.size()
            def effLen = getEffectiveLength(len)

            // A. Fused Stats (word length + starting type)
            def firstToken = structureTokens[0]
            def startType = firstToken[0]
            def fusedKey = "${len}:${startType}"
            model.lengthStartStats[fusedKey] = (model.lengthStartStats[fusedKey] ?: 0) + 1

            // B. Start Token
            def startContent = firstToken[1]
            def startMap = ensureNestedMap(model.startTokens, effLen, startType)
            startMap[startContent] = (startMap[startContent] ?: 0) + 1

            // C. Subsequent Tokens
            structureTokens.eachWithIndex { t, i ->
                if (i > 0) {
                    def prevToken = structureTokens[i - 1]
                    def prevType = prevToken[0]
                    def prevContent = prevToken[1]
                    def content = t[1]

                    // Base token model (type-based)
                    def baseTokenMap = (i == len - 1) ? model.lastTokens : model.innerTokens
                    def tokenMap = ensureNestedMap(baseTokenMap, effLen, prevType)
                    tokenMap[content] = (tokenMap[content] ?: 0) + 1

                    // Bigram modeling
                    if (ngramMode >= NGRAM_BIGRAM) {
                        def bigramKey = prevContent
                        def bigramModel
                        if (i == len - 1) {
                            bigramModel = model.bigramLast
                        } else if (i == 1) {
                            bigramModel = model.bigramStart
                        } else {
                            bigramModel = model.bigramInner
                        }
                        def bigramMap = ensureNestedMap(bigramModel, effLen, bigramKey)
                        bigramMap[content] = (bigramMap[content] ?: 0) + 1
                    }

                    // Trigram modeling
                    if (ngramMode >= NGRAM_TRIGRAM && i >= 2) {
                        def prev2Content = structureTokens[i - 2][1]
                        def trigramKey = "${prev2Content}:${prevContent}"
                        def trigramModel = (i == len - 1) ? model.trigramLast : model.trigramInner
                        def trigramMap = ensureNestedMap(trigramModel, effLen, trigramKey)
                        trigramMap[content] = (trigramMap[content] ?: 0) + 1
                    }
                }
            }
            currentSegmentLen++
            wordsInCurrentSentence++
            previousPunctuation = null
        }

        // --- 2. Analyze Sentence Structure ---
        if (punctuation) {
            // Filter: Skip consecutive punctuation
            if (previousPunctuation != null && currentSegmentLen == 0) {
                previousPunctuation = punctuation
                return
            }

            if (currentSegmentLen > 0) {
                // Record segment length
                ensureSegmentList(model, previousState).add(currentSegmentLen)

                // Record bucket-specific transition
                def segLenBucket = getSegmentLengthBucket(currentSegmentLen)
                def transKey = previousState + "@" + segLenBucket
                def transMap = ensureTransitionMap(model, transKey)
                transMap[punctuation] = (transMap[punctuation] ?: 0) + 1

                // Record state-only fallback transition
                def fallbackMap = ensureTransitionMap(model, previousState)
                fallbackMap[punctuation] = (fallbackMap[punctuation] ?: 0) + 1
            }

            currentSegmentLen = 0
            previousPunctuation = punctuation

            if (TERMINATORS.contains(punctuation)) {
                // Filter: Only record non-empty sentences
                if (wordsInCurrentSentence > 0) {
                    model.sentenceLengths.add(wordsInCurrentSentence)
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

// ----------------------------------------------------------------
// --- VALIDATION ---
// ----------------------------------------------------------------

def validateModel(Model model, boolean requireSentenceStructure) {
    def errors = []

    if (model.lengthStartSelector == null || model.lengthStartSelector.isEmpty()) {
        errors << "No word length statistics found"
    }

    model.lengthStartStats.each { fusedKey, count ->
        def parts = fusedKey.split(':')
        def len = parts[0].toInteger()
        def startType = parts[1]
        def effLen = getEffectiveLength(len)

        // Validate start token selector exists
        def startSelector = model.startSelectors[effLen]?[startType]
        if (!startSelector || startSelector.isEmpty()) {
            errors << "Missing start token selector for effLen=${effLen}, type=${startType} (fusedKey=${fusedKey}, count=${count})"
        }

        // Validate last token selector exists (for len > 1)
        if (len > 1) {
            def lastPrevType = ((len % 2) == 0) ? startType : (startType == 'C' ? 'V' : 'C')
            def lastSelector = model.lastSelectors[effLen]?[lastPrevType]
            if (!lastSelector || lastSelector.isEmpty()) {
                errors << "Missing last token selector for effLen=${effLen}, prevType=${lastPrevType} (fusedKey=${fusedKey}, count=${count})"
            }
        }

        // Validate inner token selector exists (for len > 2)
        if (len > 2) {
            def hasC = model.innerSelectors[effLen]?.get('C')
            def hasV = model.innerSelectors[effLen]?.get('V')
            if ((!hasC || hasC.isEmpty()) && (!hasV || hasV.isEmpty())) {
                errors << "Missing inner token selectors for effLen=${effLen} (fusedKey=${fusedKey}, count=${count})"
            }
        }
    }

    if (requireSentenceStructure) {
        if (model.segmentLengths.isEmpty()) {
            errors << "No segment length data found (corpus may lack punctuation)"
        }
        if (model.transitionSelectors.isEmpty()) {
            errors << "No transition data found (corpus may lack punctuation)"
        }

        model.segmentLengths.keySet().each { state ->
            if (!model.transitionSelectors[state] || model.transitionSelectors[state].isEmpty()) {
                errors << "State '${state}' has segment lengths but no fallback transition selector"
            }
        }
    }

    if (errors) {
        throw new ModelValidationException("org.tdl.pwg.org.tdl.pwg.gemini.Model validation failed:\n  - ${errors.join('\n  - ')}")
    }
}

// ----------------------------------------------------------------
// --- GENERATION ---
// ----------------------------------------------------------------

def generatePseudoWord(Model model) {
    def fusedKey = model.lengthStartSelector.select(RND)
    if (!fusedKey) {
        throw new GenerationException("Failed to select word length/type from empty lengthStartStats")
    }

    def parts = fusedKey.split(':')
    def len = parts[0].toInteger()
    def startType = parts[1]
    def effLen = getEffectiveLength(len)

    def tokenContents = []

    // 1. Start Token
    def startSelector = model.startSelectors[effLen][startType]
    def startContent = startSelector.select(RND)
    if (!startContent) {
        throw new GenerationException("Failed to select start token for effLen=${effLen}, type=${startType}")
    }
    tokenContents << startContent
    def prevType = startType

    // 2. Subsequent Tokens
    for (int i = 1; i < len; i++) {
        def requiredType = (prevType == 'C') ? 'V' : 'C'
        def tokenContent = null

        // Try N-gram selection first
        tokenContent = tryNgramSelection(model, effLen, tokenContents, (i == len - 1), i)

        // Fallback to type-based selection
        if (!tokenContent) {
            def baseSelectors = (i == len - 1) ? model.lastSelectors : model.innerSelectors
            def fallbackSelector = baseSelectors[effLen]?[prevType]
            if (!fallbackSelector || fallbackSelector.isEmpty()) {
                def mapName = (i == len - 1) ? "last" : "inner"
                throw new GenerationException("No ${mapName} token data for effLen=${effLen}, prevType=${prevType}")
            }
            tokenContent = fallbackSelector.select(RND)
        }

        if (!tokenContent) {
            throw new GenerationException("Failed to generate token at position ${i} for word length ${len}")
        }

        tokenContents << tokenContent
        prevType = requiredType
    }

    return tokenContents.join('')
}

def tryNgramSelection(Model model, int effLen, List<String> tokenContents, boolean isLast, int position) {
    if (model.ngramMode == NGRAM_NONE) return null

    def tokenContent = null

    // Try Trigram (highest priority)
    if (model.ngramMode >= NGRAM_TRIGRAM && tokenContents.size() >= 2) {
        def trigramKey = "${tokenContents[-2]}:${tokenContents[-1]}"
        def trigramSelectors = isLast ? model.trigramLastSelectors : model.trigramInnerSelectors
        def trigramSelector = trigramSelectors[effLen]?[trigramKey]
        if (trigramSelector && !trigramSelector.isEmpty()) {
            tokenContent = trigramSelector.select(RND)
        }
    }

    // Try Bigram
    if (!tokenContent && model.ngramMode >= NGRAM_BIGRAM && tokenContents.size() >= 1) {
        def bigramKey = tokenContents[-1]
        def bigramSelectors
        if (isLast) {
            bigramSelectors = model.bigramLastSelectors
        } else if (position == 1) {
            bigramSelectors = model.bigramStartSelectors
        } else {
            bigramSelectors = model.bigramInnerSelectors
        }
        def bigramSelector = bigramSelectors[effLen]?[bigramKey]
        if (bigramSelector && !bigramSelector.isEmpty()) {
            tokenContent = bigramSelector.select(RND)
        }
    }

    return tokenContent
}

def generateSentences(Model model, int targetSentences) {
    def sb = new StringBuilder()
    def sentencesGenerated = 0
    def currentState = "START"

    while (sentencesGenerated < targetSentences) {
        def lenList = model.segmentLengths[currentState]
        if (!lenList || lenList.isEmpty()) {
            throw new GenerationException("No segment length data for state '${currentState}'")
        }

        int segLen = lenList[RND.nextInt(lenList.size())]
        segLen = Math.max(1, segLen)

        def segLenBucket = getSegmentLengthBucket(segLen)
        def transKey = currentState + "@" + segLenBucket

        // Prioritize bucket-specific selector, fallback to state-only
        def nextPunctSelector = model.transitionSelectors[transKey]
        if (!nextPunctSelector || nextPunctSelector.isEmpty()) {
            nextPunctSelector = model.transitionSelectors[currentState]
        }

        if (!nextPunctSelector || nextPunctSelector.isEmpty()) {
            throw new GenerationException("No transition data for state '${currentState}'")
        }

        def nextPunct = nextPunctSelector.select(RND)

        def segmentWords = []
        segLen.times { segmentWords << generatePseudoWord(model) }

        if (currentState == "START") {
            segmentWords[0] = segmentWords[0].capitalize()
        }
        sb.append(segmentWords.join(" ")).append(nextPunct).append(" ")

        if (TERMINATORS.contains(nextPunct)) {
            currentState = "START"
            sentencesGenerated++
        } else {
            currentState = nextPunct
        }
    }

    println sb.toString().trim().replaceAll("(.{1,80})\\s+", "\$1\n")
}

def generateWordList(Model model, int count) {
    count.times { println generatePseudoWord(model) }
}

// ----------------------------------------------------------------
// --- STATISTICS ---
// ----------------------------------------------------------------

def printStatistics(Model model, boolean sentenceMode) {
    println "\n📊 --- ANALYSIS REPORT ---"

    def ngramLabel = ["None", "Bigram", "Trigram"][model.ngramMode]
    println "📝 N-gram modeling: ${ngramLabel}"

    def totalWords = model.lengthStartStats.values().sum() ?: 1

    println "\n[Word Length Distribution]"
    println String.format("%-8s | %-14s | %-14s | %-8s", "Len", "Start: C", "Start: V", "Total")
    println "-" * 54

    def bucketedStats = [:]
    model.lengthStartStats.each { fusedKey, count ->
        def parts = fusedKey.split(':')
        def len = parts[0].toInteger()
        def type = parts[1]
        def bucket = getEffectiveLength(len)

        if (!bucketedStats.containsKey(bucket)) {
            bucketedStats[bucket] = [c: 0, v: 0]
        }
        if (type == 'C') bucketedStats[bucket].c += count
        else bucketedStats[bucket].v += count
    }

    bucketedStats.keySet().sort().each { len ->
        def cCount = bucketedStats[len].c
        def vCount = bucketedStats[len].v
        def sum = cCount + vCount
        def cPct = (cCount / totalWords) * 100
        def vPct = (vCount / totalWords) * 100
        def lenLabel = (len >= MAX_EFF_LEN) ? "${len}+" : "${len}"
        println String.format("%-8s | %6d (%5.1f%%) | %6d (%5.1f%%) | %6d", lenLabel, cCount, cPct, vCount, vPct, sum)
    }

    if (sentenceMode) {
        println "\n[Sentence Structure]"

        if (model.sentenceLengths && !model.sentenceLengths.isEmpty()) {
            def avgSentLen = model.sentenceLengths.sum() / model.sentenceLengths.size()
            def minSentLen = model.sentenceLengths.min()
            def maxSentLen = model.sentenceLengths.max()
            println String.format("Sentence length: Avg %.1f, Min %d, Max %d words (n=%d)",
                    avgSentLen, minSentLen, maxSentLen, model.sentenceLengths.size())
        }

        println "\n[Segment Lengths by State]"
        model.segmentLengths.keySet().sort().each { state ->
            def lenList = model.segmentLengths[state]
            if (lenList && !lenList.isEmpty()) {
                def avg = lenList.sum() / lenList.size()
                def stateLabel = (state == "START") ? "Start" : "After '${state}'"
                println String.format("%-15s : Avg %.1f words (n=%d)", stateLabel, avg, lenList.size())
            }
        }

        println "\n[Transition Probabilities by State (fallback)]"
        model.transitions.keySet().findAll { !it.contains('@') }.sort().each { state ->
            def transMap = model.transitions[state]
            if (transMap && !transMap.isEmpty()) {
                def total = transMap.values().sum()
                def stateLabel = (state == "START") ? "Start" : "After '${state}'"
                def probs = transMap.collect { punct, count ->
                    "'${punct}':" + String.format("%.1f%%", (count / total) * 100)
                }.join(", ")
                println String.format("%-15s → %s (n=%d)", stateLabel, probs, total)
            }
        }

        println "\n[Transition Probabilities by State and Segment Length]"
        model.transitions.keySet().findAll { it.contains('@') }.sort().each { key ->
            def transMap = model.transitions[key]
            if (transMap && !transMap.isEmpty()) {
                def total = transMap.values().sum()
                def parts = key.split('@')
                def state = parts[0]
                def segBucket = parts[1]
                def stateLabel = (state == "START") ? "Start" : "After '${state}'"

                def termCount = (transMap['.'] ?: 0) + (transMap['!'] ?: 0) + (transMap['?'] ?: 0)
                def termPct = (termCount / total) * 100

                def probs = transMap.collect { punct, count ->
                    "'${punct}':" + String.format("%.1f%%", (count / total) * 100)
                }.join(", ")
                println String.format("%-15s [%4s words] → %s (n=%d, term=%.1f%%)", stateLabel, segBucket, probs, total, termPct)
            }
        }
    }
    println "\n" + ("-" * 54) + "\n"
}

def printUsage() {
    println """
Usage:
  groovy PseudoGenerator.groovy -f <filename> -s <sentences> [-b | -t]
  groovy PseudoGenerator.groovy -f <filename> -w <words> [-b | -t] [-p <n>]

Required options:
  -f <file>   Input corpus file
  -s <n>      Generate n sentences of pseudo-text
  -w <n>      Generate n individual pseudo-words

Optional n-gram modeling:
  -b          Enable bigram cluster modeling
  -t          Enable trigram cluster modeling (includes bigrams)

Optional word filtering (word mode only):
  -p <n>      Prune: only generate words with n or more tokens
              (e.g., -p 3 excludes 1-token and 2-token words)

Options can appear in any order.

Examples:
  groovy PseudoGenerator.groovy -f corpus.txt -s 10
  groovy PseudoGenerator.groovy -s 20 -f novel.txt -b
  groovy PseudoGenerator.groovy -t -w 50 -f names.csv
  groovy PseudoGenerator.groovy -f names.csv -w 100 -p 3 -b
"""
}

// ----------------------------------------------------------------
// --- MAIN ---
// ----------------------------------------------------------------

def cmdArgs = parseCommandLine(args)

if (!cmdArgs.valid) {
    println "❌ Error: ${cmdArgs.errorMessage}"
    printUsage()
    System.exit(1)
}

try {
    println "📖 Analyzing corpus: ${cmdArgs.filePath}"
    def model = analyzeText(cmdArgs.filePath, cmdArgs.ngramMode)

    // Apply pruning if requested (before building selectors)
    if (cmdArgs.pruneMinTokens > 0) {
        pruneModel(model, cmdArgs.pruneMinTokens)
    }

    // Build optimized selectors (after pruning, so only valid entries are included)
    buildModelSelectors(model)

    def requireSentenceStructure = (cmdArgs.mode == '-s')
    println "✅ Analysis complete. Validating model..."
    validateModel(model, requireSentenceStructure)
    println "✅ org.tdl.pwg.org.tdl.pwg.gemini.Model validation passed."

    printStatistics(model, cmdArgs.mode == '-s')

    if (cmdArgs.mode == '-w') {
        println "✍️  GENERATED WORDS:"
        println "=" * 60
        generateWordList(model, cmdArgs.count)
        println "=" * 60
    } else if (cmdArgs.mode == '-s') {
        println "✍️  GENERATED TEXT:"
        println "=" * 60
        generateSentences(model, cmdArgs.count)
        println "=" * 60
    }

} catch (ModelValidationException e) {
    println "❌ MODEL VALIDATION FAILED:"
    println e.message
    System.exit(2)
} catch (GenerationException e) {
    println "❌ GENERATION FAILED:"
    println e.message
    System.exit(3)
} catch (Exception e) {
    println "❌ UNEXPECTED ERROR: ${e.message}"
    e.printStackTrace()
    System.exit(1)
}