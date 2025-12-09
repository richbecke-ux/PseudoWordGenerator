package org.tdl.pwg

import groovy.transform.Field
import java.nio.file.Files
import java.nio.file.Paths

// --- Configuration ---

@Field final Random RND = new Random()
@Field final Set<String> VOWELS = new HashSet(['a', 'e', 'i', 'o', 'u', 'y', 'æ', 'ø', 'å', 'ä', 'ö', 'ü', 'é'])
@Field final Set<String> TERMINATORS = new HashSet(['.', '!', '?'])
@Field final Set<String> INTRA_PUNCT = new HashSet([',', ':', ';'])
@Field final Set<String> ALL_PUNCT = TERMINATORS + INTRA_PUNCT

@Field final int MAX_EFF_LEN = 8
@Field final List<String> SEG_LEN_BUCKETS = ["1", "2", "3", "4-5", "6-8", "9+"]

// N-gram modes
@Field final int NGRAM_NONE = 0
@Field final int NGRAM_BIGRAM = 1
@Field final int NGRAM_TRIGRAM = 2

// --- Data Structures ---

class Model {
    Map<String, Integer> lengthStartStats = [:]
    Map<Integer, Map<String, Map<String, Integer>>> startTokens = [:]
    Map<Integer, Map<String, Map<String, Integer>>> innerTokens = [:]
    Map<Integer, Map<String, Map<String, Integer>>> lastTokens = [:]
    Map<String, List<Integer>> segmentLengths = [:]
    Map<String, Map<String, Integer>> transitions = [:]
    List<Integer> sentenceLengths = []

    // N-gram models
    Map<Integer, Map<String, Map<String, Integer>>> bigramStart = [:]
    Map<Integer, Map<String, Map<String, Integer>>> bigramInner = [:]
    Map<Integer, Map<String, Map<String, Integer>>> bigramLast = [:]
    Map<Integer, Map<String, Map<String, Integer>>> trigramInner = [:]
    Map<Integer, Map<String, Map<String, Integer>>> trigramLast = [:]

    int ngramMode = 0  // NGRAM_NONE, NGRAM_BIGRAM, or NGRAM_TRIGRAM
}

class CommandLineArgs {
    String filePath = null
    String mode = null      // "-s" or "-w"
    int count = 0
    int ngramMode = 0       // NGRAM_NONE, NGRAM_BIGRAM, or NGRAM_TRIGRAM
    boolean valid = false
    String errorMessage = null
}

class ModelValidationException extends RuntimeException {
    ModelValidationException(String message) { super(message) }
}

class GenerationException extends RuntimeException {
    GenerationException(String message) { super(message) }
}

// --- Command Line Parsing ---

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
                if (i + 1 >= args.length) {
                    result.errorMessage = "Option -s requires a number argument"
                    return result
                }
                if (result.mode != null) {
                    result.errorMessage = "Cannot specify both -s and -w"
                    return result
                }
                result.mode = '-s'
                try {
                    result.count = args[++i].toInteger()
                } catch (NumberFormatException e) {
                    result.errorMessage = "Invalid number for -s: ${args[i]}"
                    return result
                }
                break

            case '-w':
                if (i + 1 >= args.length) {
                    result.errorMessage = "Option -w requires a number argument"
                    return result
                }
                if (result.mode != null) {
                    result.errorMessage = "Cannot specify both -s and -w"
                    return result
                }
                result.mode = '-w'
                try {
                    result.count = args[++i].toInteger()
                } catch (NumberFormatException e) {
                    result.errorMessage = "Invalid number for -w: ${args[i]}"
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

            default:
                result.errorMessage = "Unknown option: ${arg}"
                return result
        }
        i++
    }

    // Validate required arguments
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

    result.valid = true
    return result
}

// --- Helper Functions ---

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

def tokenizeWordStructure(String word) {
    def cleanWord = word.findAll { Character.isLetter(it as char) }.join('')
    if (!cleanWord) return []

    def tokens = []
    def currentType = ''
    def currentContent = ''

    cleanWord.each { c ->
        def charStr = c.toString()
        def isVowel = VOWELS.contains(charStr)
        def charType = isVowel ? 'V' : 'C'

        if (currentType == '') {
            currentType = charType
            currentContent = charStr
        } else if (charType == currentType) {
            currentContent += charStr
        } else {
            tokens << [type: currentType, content: currentContent]
            currentType = charType
            currentContent = charStr
        }
    }
    if (currentContent) tokens << [type: currentType, content: currentContent]
    return tokens
}

def selectFromWeightedMap(Map<String, Integer> map) {
    if (!map || map.isEmpty()) return null
    long total = map.values().sum()
    if (total == 0) return null
    double r = RND.nextDouble() * total
    double running = 0.0
    for (entry in map) {
        running += entry.value
        if (running >= r) return entry.key
    }
    return map.keySet().last()
}

// --- Proactive Map/List Access ---

def ensureStartTokenMap(Model model, int effLen, String type) {
    if (!model.startTokens.containsKey(effLen)) {
        model.startTokens[effLen] = new HashMap<String, Map<String, Integer>>()
    }
    if (!model.startTokens[effLen].containsKey(type)) {
        model.startTokens[effLen][type] = new HashMap<String, Integer>()
    }
    return model.startTokens[effLen][type]
}

def ensureInnerTokenMap(Model model, int effLen, String prevType) {
    if (!model.innerTokens.containsKey(effLen)) {
        model.innerTokens[effLen] = new HashMap<String, Map<String, Integer>>()
    }
    if (!model.innerTokens[effLen].containsKey(prevType)) {
        model.innerTokens[effLen][prevType] = new HashMap<String, Integer>()
    }
    return model.innerTokens[effLen][prevType]
}

def ensureLastTokenMap(Model model, int effLen, String prevType) {
    if (!model.lastTokens.containsKey(effLen)) {
        model.lastTokens[effLen] = new HashMap<String, Map<String, Integer>>()
    }
    if (!model.lastTokens[effLen].containsKey(prevType)) {
        model.lastTokens[effLen][prevType] = new HashMap<String, Integer>()
    }
    return model.lastTokens[effLen][prevType]
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

def ensureBigramMap(Map<Integer, Map<String, Map<String, Integer>>> bigramModel, int effLen, String key) {
    if (!bigramModel.containsKey(effLen)) {
        bigramModel[effLen] = new HashMap<String, Map<String, Integer>>()
    }
    if (!bigramModel[effLen].containsKey(key)) {
        bigramModel[effLen][key] = new HashMap<String, Integer>()
    }
    return bigramModel[effLen][key]
}

def ensureTrigramMap(Map<Integer, Map<String, Map<String, Integer>>> trigramModel, int effLen, String key) {
    if (!trigramModel.containsKey(effLen)) {
        trigramModel[effLen] = new HashMap<String, Map<String, Integer>>()
    }
    if (!trigramModel[effLen].containsKey(key)) {
        trigramModel[effLen][key] = new HashMap<String, Integer>()
    }
    return trigramModel[effLen][key]
}

// --- Analysis ---

def analyzeText(String filePath, int ngramMode) {
    def text = Files.readString(Paths.get(filePath)).toLowerCase()
    def rawTokens = text.split(/\s+/)
    def model = new Model()
    model.ngramMode = ngramMode

    def currentSegmentLen = 0
    def wordsInCurrentSentence = 0
    def previousState = "START"
    def previousPunctuation = null  // Track for consecutive punctuation filtering

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

            // A. Fused Stats
            def firstToken = structureTokens[0]
            def startType = firstToken.type
            def fusedKey = "${len}:${startType}"
            model.lengthStartStats[fusedKey] = (model.lengthStartStats[fusedKey] ?: 0) + 1

            // B. Start Token
            def startContentMap = ensureStartTokenMap(model, effLen, startType)
            def startContent = firstToken.content
            startContentMap[startContent] = (startContentMap[startContent] ?: 0) + 1

            // C. Subsequent Tokens
            structureTokens.eachWithIndex { t, i ->
                if (i > 0) {
                    def prevType = structureTokens[i-1].type
                    def prevContent = structureTokens[i-1].content
                    def content = t.content
                    def bigramKey = prevContent
                    def trigramKey = (i >= 2) ? "${structureTokens[i-2].content}:${prevContent}" : null

                    if (i == len - 1) {
                        // LAST TOKEN
                        def lastMap = ensureLastTokenMap(model, effLen, prevType)
                        lastMap[content] = (lastMap[content] ?: 0) + 1

                        if (ngramMode >= NGRAM_BIGRAM) {
                            def bigramMap = ensureBigramMap(model.bigramLast, effLen, bigramKey)
                            bigramMap[content] = (bigramMap[content] ?: 0) + 1
                        }
                        if (ngramMode >= NGRAM_TRIGRAM && trigramKey) {
                            def trigramMap = ensureTrigramMap(model.trigramLast, effLen, trigramKey)
                            trigramMap[content] = (trigramMap[content] ?: 0) + 1
                        }
                    } else {
                        // INNER TOKEN
                        def innerMap = ensureInnerTokenMap(model, effLen, prevType)
                        innerMap[content] = (innerMap[content] ?: 0) + 1

                        if (ngramMode >= NGRAM_BIGRAM) {
                            if (i == 1) {
                                def bigramMap = ensureBigramMap(model.bigramStart, effLen, bigramKey)
                                bigramMap[content] = (bigramMap[content] ?: 0) + 1
                            } else {
                                def bigramMap = ensureBigramMap(model.bigramInner, effLen, bigramKey)
                                bigramMap[content] = (bigramMap[content] ?: 0) + 1
                            }
                        }
                        if (ngramMode >= NGRAM_TRIGRAM && trigramKey) {
                            def trigramMap = ensureTrigramMap(model.trigramInner, effLen, trigramKey)
                            trigramMap[content] = (trigramMap[content] ?: 0) + 1
                        }
                    }
                }
            }
            currentSegmentLen++
            wordsInCurrentSentence++
            previousPunctuation = null  // Reset consecutive punctuation tracker
        }

        // --- 2. Analyze Sentence Structure ---
        if (punctuation) {
            // Filter: Skip if this is consecutive punctuation (no words between)
            if (previousPunctuation != null && currentSegmentLen == 0) {
                // Consecutive punctuation - skip recording
                previousPunctuation = punctuation
                return
            }

            if (currentSegmentLen > 0) {
                // Record segment length for this state
                ensureSegmentList(model, previousState).add(currentSegmentLen)

                // Record transition keyed by state AND segment length bucket
                def segLenBucket = getSegmentLengthBucket(currentSegmentLen)
                def transKey = previousState + "@" + segLenBucket
                def transMap = ensureTransitionMap(model, transKey)
                transMap[punctuation] = (transMap[punctuation] ?: 0) + 1

                // Also record state-only fallback transition
                def fallbackMap = ensureTransitionMap(model, previousState)
                fallbackMap[punctuation] = (fallbackMap[punctuation] ?: 0) + 1
            }

            currentSegmentLen = 0
            previousPunctuation = punctuation

            if (TERMINATORS.contains(punctuation)) {
                // Filter: Only record sentence length if > 0
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

// --- Validation ---

def validateModel(Model model, boolean requireSentenceStructure) {
    def errors = []

    if (model.lengthStartStats.isEmpty()) {
        errors << "No word length statistics found"
    }

    model.lengthStartStats.each { fusedKey, count ->
        def parts = fusedKey.split(':')
        def len = parts[0].toInteger()
        def startType = parts[1]
        def effLen = getEffectiveLength(len)

        if (!model.startTokens[effLen]?[startType] || model.startTokens[effLen][startType].isEmpty()) {
            errors << "Missing start tokens for effLen=${effLen}, type=${startType} (fusedKey=${fusedKey}, count=${count})"
        }

        if (len > 1) {
            def lastPrevType = ((len % 2) == 0) ? startType : (startType == 'C' ? 'V' : 'C')
            if (!model.lastTokens[effLen]?[lastPrevType] || model.lastTokens[effLen][lastPrevType].isEmpty()) {
                errors << "Missing last tokens for effLen=${effLen}, prevType=${lastPrevType} (fusedKey=${fusedKey}, count=${count})"
            }
        }

        if (len > 2) {
            def hasC = model.innerTokens[effLen]?['C'] && !model.innerTokens[effLen]['C'].isEmpty()
            def hasV = model.innerTokens[effLen]?['V'] && !model.innerTokens[effLen]['V'].isEmpty()
            if (!hasC && !hasV) {
                errors << "Missing inner tokens for effLen=${effLen} (fusedKey=${fusedKey}, count=${count})"
            }
        }
    }

    if (requireSentenceStructure) {
        if (model.segmentLengths.isEmpty()) {
            errors << "No segment length data found (corpus may lack punctuation)"
        }
        if (model.transitions.isEmpty()) {
            errors << "No transition data found (corpus may lack punctuation)"
        }

        model.segmentLengths.keySet().each { state ->
            if (!model.transitions[state] || model.transitions[state].isEmpty()) {
                errors << "State '${state}' has segment lengths but no fallback transitions"
            }
        }
    }

    if (errors) {
        throw new ModelValidationException("org.tdl.pwg.org.tdl.pwg.Model validation failed:\n  - ${errors.join('\n  - ')}")
    }
}

// --- Generation ---

def generatePseudoWord(Model model) {
    def fusedKey = selectFromWeightedMap(model.lengthStartStats)
    if (!fusedKey) {
        throw new GenerationException("Failed to select word length/type from empty lengthStartStats")
    }

    def parts = fusedKey.split(':')
    def len = parts[0].toInteger()
    def startType = parts[1]
    def effLen = getEffectiveLength(len)

    def tokens = []
    def tokenContents = []

    def startContentMap = model.startTokens[effLen][startType]
    def startContent = selectFromWeightedMap(startContentMap)
    if (!startContent) {
        throw new GenerationException("Failed to select start token for effLen=${effLen}, type=${startType}")
    }
    tokens << startContent
    tokenContents << startContent
    def prevType = startType

    for (int i = 1; i < len; i++) {
        def requiredType = (prevType == 'C') ? 'V' : 'C'
        def tokenContent = null

        if (i == len - 1) {
            tokenContent = tryNgramSelection(model, effLen, tokenContents, true, i)
            if (!tokenContent) {
                def lastMap = model.lastTokens[effLen]?[prevType]
                if (!lastMap || lastMap.isEmpty()) {
                    throw new GenerationException("No last token data for effLen=${effLen}, prevType=${prevType}")
                }
                tokenContent = selectFromWeightedMap(lastMap)
            }
        } else {
            tokenContent = tryNgramSelection(model, effLen, tokenContents, false, i)
            if (!tokenContent) {
                def innerMap = model.innerTokens[effLen]?[prevType]
                if (!innerMap || innerMap.isEmpty()) {
                    throw new GenerationException("No inner token data for effLen=${effLen}, prevType=${prevType}")
                }
                tokenContent = selectFromWeightedMap(innerMap)
            }
        }

        if (!tokenContent) {
            throw new GenerationException("Failed to generate token at position ${i} for word length ${len}")
        }

        tokens << tokenContent
        tokenContents << tokenContent
        prevType = requiredType
    }

    return tokens.join('')
}

def tryNgramSelection(Model model, int effLen, List<String> tokenContents, boolean isLast, int position) {
    if (model.ngramMode == NGRAM_NONE) return null

    def tokenContent = null

    // Try trigram (only if trigram mode enabled and we have 2+ previous tokens)
    if (model.ngramMode >= NGRAM_TRIGRAM && tokenContents.size() >= 2) {
        def trigramKey = "${tokenContents[-2]}:${tokenContents[-1]}"
        def trigramModel = isLast ? model.trigramLast : model.trigramInner
        def trigramMap = trigramModel[effLen]?[trigramKey]
        if (trigramMap && !trigramMap.isEmpty()) {
            tokenContent = selectFromWeightedMap(trigramMap)
        }
    }

    // Try bigram (if bigram or trigram mode enabled)
    if (!tokenContent && model.ngramMode >= NGRAM_BIGRAM && tokenContents.size() >= 1) {
        def bigramKey = tokenContents[-1]
        def bigramModel
        if (isLast) {
            bigramModel = model.bigramLast
        } else if (position == 1) {
            bigramModel = model.bigramStart
        } else {
            bigramModel = model.bigramInner
        }
        def bigramMap = bigramModel[effLen]?[bigramKey]
        if (bigramMap && !bigramMap.isEmpty()) {
            tokenContent = selectFromWeightedMap(bigramMap)
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
        def nextPunctMap = model.transitions[transKey]

        if (!nextPunctMap || nextPunctMap.isEmpty()) {
            nextPunctMap = model.transitions[currentState]
        }

        if (!nextPunctMap || nextPunctMap.isEmpty()) {
            throw new GenerationException("No transition data for state '${currentState}'")
        }

        def nextPunct = selectFromWeightedMap(nextPunctMap)

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
    println sb.toString().replaceAll("(.{1,80})\\s+", "\$1\n")
}

def generateWordList(Model model, int count) {
    count.times { println generatePseudoWord(model) }
}

// --- Statistics ---

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
                    "'${punct}':" + String.format("%.1f%%", (count/total)*100)
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
                    "'${punct}':" + String.format("%.1f%%", (count/total)*100)
                }.join(", ")
                println String.format("%-15s [%4s words] → %s (n=%d, term=%.1f%%)", stateLabel, segBucket, probs, total, termPct)
            }
        }
    }
    println "\n" + ("-"*54) + "\n"
}

def printUsage() {
    println """
Usage:
  groovy PseudoGenerator.groovy -f <filename> -s <sentences> [-b | -t]
  groovy PseudoGenerator.groovy -f <filename> -w <words> [-b | -t]

Required options:
  -f <file>   Input corpus file
  -s <n>      Generate n sentences of pseudo-text
  -w <n>      Generate n individual pseudo-words

Optional n-gram modeling:
  -b          Enable bigram cluster modeling
  -t          Enable trigram cluster modeling (includes bigrams)

Options can appear in any order.

Examples:
  groovy PseudoGenerator.groovy -f corpus.txt -s 10
  groovy PseudoGenerator.groovy -s 20 -f novel.txt -b
  groovy PseudoGenerator.groovy -t -w 50 -f names.csv
"""
}

// --- Main ---

def cmdArgs = parseCommandLine(args)

if (!cmdArgs.valid) {
    println "❌ Error: ${cmdArgs.errorMessage}"
    printUsage()
    System.exit(1)
}

try {
    println "📖 Analyzing corpus: ${cmdArgs.filePath}"
    def model = analyzeText(cmdArgs.filePath, cmdArgs.ngramMode)

    def requireSentenceStructure = (cmdArgs.mode == '-s')
    println "✅ Analysis complete. Validating model..."
    validateModel(model, requireSentenceStructure)
    println "✅ org.tdl.pwg.org.tdl.pwg.Model validation passed."

    printStatistics(model, cmdArgs.mode == '-s')

    if (cmdArgs.mode == '-w') {
        println "✍️  GENERATED WORDS:"
        println "="*60
        generateWordList(model, cmdArgs.count)
        println "="*60
    } else if (cmdArgs.mode == '-s') {
        println "✍️  GENERATED TEXT:"
        println "="*60
        generateSentences(model, cmdArgs.count)
        println "="*60
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