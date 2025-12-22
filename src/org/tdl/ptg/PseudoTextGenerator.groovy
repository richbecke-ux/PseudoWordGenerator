package org.tdl.ptg

import groovy.transform.Field
import groovy.transform.Canonical
import java.nio.file.Files
import java.nio.file.Paths

// ----------------------------------------------------------------
// --- CONFIGURATION ---
// ----------------------------------------------------------------

@Field final Random RND = new Random()
@Field final Set<String> VOWELS = new HashSet(['a', 'e', 'i', 'o', 'u', 'y', 'æ', 'ø', 'å', 'ä', 'ö', 'ü', 'é'])
@Field final Set<String> TERMINATORS = new HashSet(['.', '!', '?'])
@Field final Set<String> INTRA_PUNCT = new HashSet([',', ':', ';', '—', '-'])
@Field final Set<String> ALL_PUNCT = TERMINATORS + INTRA_PUNCT

// Probability (0.0 - 1.0) to ignore a valid Trigram and force a Bigram choice.
// Higher = more random gibberish, Lower = closer to original style.
@Field final double CHAOS_FACTOR = 0.15

// Max attempts to generate a unique word before giving up
@Field final int MAX_UNIQUE_RETRIES = 50

@Field final int MAX_EFF_LEN = 8
@Field final int NGRAM_NONE = 0
@Field final int NGRAM_BIGRAM = 1
@Field final int NGRAM_TRIGRAM = 2

// ----------------------------------------------------------------
// --- DATA STRUCTURES ---
// ----------------------------------------------------------------

/**
 * Implements O(log N) weighted random selection.
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
    int ngramMode = 2 // Default to Trigram
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

    // Segment Rhythm Templates
    // Stores lengths of words between ANY punctuation (e.g. "Hi," -> [2])
    List<List<Integer>> segmentTemplates = []

    // Transition probabilities for punctuation
    Map<String, Map<String, Integer>> transitions = [:]

    int ngramMode = 2

    // Selectors
    WeightedSelector lengthStartSelector
    Map<Integer, WeightedSelector> lengthToStartTypeSelector = [:]

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
// --- ANALYSIS ---
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
            case '-b': result.ngramMode = NGRAM_BIGRAM; break
            case '-t': result.ngramMode = NGRAM_TRIGRAM; break
            case '-u': result.uniqueMode = true; break
            case '-nu': result.uniqueMode = false; break
        }
        i++
    }
    if (result.filePath && result.mode && result.count > 0) result.valid = true
    else result.errorMessage = "Missing arguments. Need -f, and -s or -w."
    return result
}

def getEffectiveLength(int len) { Math.min(len, MAX_EFF_LEN) }

def tokenizeWordStructure(String word) {
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
    def previousState = "START"

    // Stores lengths of words in the CURRENT SEGMENT (Clause), not full sentence
    List<Integer> currentSegmentStructure = []

    rawTokens.each { token ->
        if (!token) return

        def lastChar = token.takeRight(1)
        def punctuation = null
        def wordPart = token

        // 1. Separate punctuation first
        if (ALL_PUNCT.contains(lastChar)) {
            punctuation = lastChar
            wordPart = token.dropRight(1)
        }

        // --- SANITY FILTER ---
        def cleanPart = wordPart.findAll { Character.isLetter(it as char) }.join('')
        def hasVowel = cleanPart.any { VOWELS.contains(it.toString()) }
        def maxCons = 0
        def curCons = 0
        cleanPart.each { c ->
            if (VOWELS.contains(c.toString())) curCons = 0
            else {
                curCons++
                maxCons = Math.max(maxCons, curCons)
            }
        }

        // Only analyze valid-looking words
        if (cleanPart && hasVowel && maxCons <= 4 && cleanPart.length() <= 15) {

            def structureTokens = tokenizeWordStructure(wordPart)
            if (structureTokens) {
                model.vocabulary.add(cleanPart)

                def len = structureTokens.size()
                currentSegmentStructure.add(len) // Add to current clause

                def effLen = getEffectiveLength(len)
                def firstToken = structureTokens[0]
                def startType = firstToken[0]

                def fusedKey = "${len}:${startType}"
                model.lengthStartStats[fusedKey] = (model.lengthStartStats[fusedKey] ?: 0) + 1

                structureTokens.eachWithIndex { t, i ->
                    def content = t[1]
                    if (i == 0) {
                        ensureNestedMap(model.startTokens, effLen, startType)[content] =
                                (ensureNestedMap(model.startTokens, effLen, startType)[content] ?: 0) + 1
                    } else {
                        def prevToken = structureTokens[i - 1]
                        def prevContent = prevToken[1]
                        def prevType = prevToken[0]

                        def baseMap = (i == len - 1) ? model.lastTokens : model.innerTokens
                        ensureNestedMap(baseMap, effLen, prevType)[content] =
                                (ensureNestedMap(baseMap, effLen, prevType)[content] ?: 0) + 1

                        def bgMap = (i == len - 1) ? model.bigramLast :
                                (i == 1) ? model.bigramStart : model.bigramInner
                        ensureNestedMap(bgMap, effLen, prevContent)[content] =
                                (ensureNestedMap(bgMap, effLen, prevContent)[content] ?: 0) + 1

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
            }
        }

        // --- SEGMENT & TRANSITION ANALYSIS ---
        if (punctuation) {
            // Record Transition
            if (currentSegmentLen > 0) {
                def transMap = model.transitions.computeIfAbsent(previousState, { [:] })
                transMap[punctuation] = (transMap[punctuation] ?: 0) + 1
            }

            // Record Rhythm (Flush segment)
            if (!currentSegmentStructure.isEmpty()) {
                model.segmentTemplates.add(new ArrayList(currentSegmentStructure))
                currentSegmentStructure.clear()
            }

            currentSegmentLen = 0
            if (TERMINATORS.contains(punctuation)) {
                previousState = "START"
            } else {
                previousState = punctuation
            }
        }
    }
    return model
}

def buildModelSelectors(Model model) {
    // 1. Build specific start selectors for each word length
    Map<Integer, Map<String, Integer>> lengthToStartMap = [:]
    model.lengthStartStats.each { key, count ->
        def parts = key.split(':')
        def len = parts[0].toInteger()
        def type = parts[1]
        lengthToStartMap.computeIfAbsent(len, { [:] })[type] = count
    }
    lengthToStartMap.each { len, map ->
        model.lengthToStartTypeSelector[len] = new WeightedSelector(map)
    }

    // 2. Standard Selectors
    model.lengthStartSelector = new WeightedSelector(model.lengthStartStats)

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

def generateRawWord(Model model, int targetLength) {
    // 1. Determine Start Type for this specific length
    def typeSelector = model.lengthToStartTypeSelector[targetLength]

    // Fallback if we requested a length we've never seen
    if (!typeSelector) {
        def fusedKey = model.lengthStartSelector.select(RND)
        if (!fusedKey) return "blob"
        targetLength = fusedKey.split(':')[0].toInteger()
        def startType = fusedKey.split(':')[1]
        typeSelector = new WeightedSelector([(startType): 1])
    }

    def startType = typeSelector.select(RND)
    def effLen = getEffectiveLength(targetLength)

    def tokens = []

    // 2. Start Token
    def startContent = model.startSelectors[effLen]?[startType]?.select(RND)
    if (!startContent) return "err"
    tokens << startContent
    def prevType = startType

    // 3. Build Word
    for (int i = 1; i < targetLength; i++) {
        def isLast = (i == targetLength - 1)
        def token = null

        // A. Trigram Attempt (with Chaos Factor backoff)
        if (model.ngramMode >= NGRAM_TRIGRAM && tokens.size() >= 2) {
            if (RND.nextDouble() > CHAOS_FACTOR) {
                def tgKey = "${tokens[-2]}:${tokens[-1]}"
                def sel = isLast ? model.trigramLastSelectors : model.trigramInnerSelectors
                token = sel[effLen]?[tgKey]?.select(RND)
            }
        }

        // B. Bigram Attempt
        if (!token && model.ngramMode >= NGRAM_BIGRAM) {
            def bgKey = tokens[-1]
            def sel
            if (isLast) sel = model.bigramLastSelectors
            else if (i == 1) sel = model.bigramStartSelectors
            else sel = model.bigramInnerSelectors
            token = sel[effLen]?[bgKey]?.select(RND)
        }

        // C. Fallback (Type-based)
        if (!token) {
            def sel = isLast ? model.lastSelectors : model.innerSelectors
            token = sel[effLen]?[prevType]?.select(RND)
        }

        if (!token) token = (prevType == 'C') ? 'a' : 'b' // Emergency fallback
        tokens << token
        prevType = (prevType == 'C' ? 'V' : 'C')
    }

    return tokens.join('')
}

def generateWord(Model model, int targetLength, boolean uniqueMode) {
    int attempts = 0
    String word
    while (attempts < MAX_UNIQUE_RETRIES) {
        word = generateRawWord(model, targetLength)
        if (uniqueMode && model.vocabulary.contains(word)) {
            attempts++
            continue
        }
        return word
    }
    return word
}

def generateSentences(Model model, int count, boolean uniqueMode) {
    def sb = new StringBuilder()
    def currentState = "START"

    count.times {
        if (model.segmentTemplates.isEmpty()) {
            println "Error: No segment templates found. Input text may lack punctuation."
            return
        }

        // 1. Pick a Segment Template (Rhythm of a clause)
        def template = model.segmentTemplates[RND.nextInt(model.segmentTemplates.size())]

        // 2. Generate Words
        def words = []
        template.each { len ->
            words << generateWord(model, len, uniqueMode)
        }

        // 3. Capitalize ONLY if we are at the start of a sentence
        if (words && (currentState == "START" || TERMINATORS.contains(currentState))) {
            words[0] = words[0].capitalize()
        }

        // 4. Append
        sb.append(words.join(" "))

        // 5. Determine Punctuation
        def punctSel = model.transitionSelectors[currentState]
        if (!punctSel) punctSel = model.transitionSelectors["START"]

        def punct = punctSel ? punctSel.select(RND) : "."
        sb.append(punct).append(" ")

        // 6. Update State
        if (TERMINATORS.contains(punct)) currentState = "START"
        else currentState = punct
    }

    println sb.toString().trim().replaceAll("(.{1,80})\\s+", "\$1\n")
}

def generateRandomWords(Model model, int count, boolean uniqueMode) {
    count.times {
        def fusedKey = model.lengthStartSelector.select(RND)
        if (fusedKey) {
            def len = fusedKey.split(':')[0].toInteger()
            println generateWord(model, len, uniqueMode)
        }
    }
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

println "📖 Analyzing ${argsObj.filePath}..."
println "   (Mode: Trigram + Chaos Backoff + Rhythm Mimicry)"
def model = analyzeText(argsObj.filePath, argsObj.ngramMode)
buildModelSelectors(model)

println "📊 Stats: ${model.vocabulary.size()} unique words found."
println "   ${model.segmentTemplates.size()} segment rhythm templates captured."

if (argsObj.uniqueMode) println "✨ Unique Mode: Active (Filtering exact dictionary matches)"

println "\n✍️  OUTPUT:"
println "=" * 60

if (argsObj.mode == '-w') {
    generateRandomWords(model, argsObj.count, argsObj.uniqueMode)
} else {
    generateSentences(model, argsObj.count, argsObj.uniqueMode)
}
println "=" * 60