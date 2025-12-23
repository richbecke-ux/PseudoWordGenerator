package org.tdl.ptg.names

import groovy.transform.Field
import groovy.transform.Canonical
import java.nio.file.Files
import java.nio.file.Paths

// ----------------------------------------------------------------
// --- CONFIGURATION ---
// ----------------------------------------------------------------

@Field final Random RND = new Random()
@Field final Set<String> VOWELS = new HashSet(['a', 'e', 'i', 'o', 'u', 'y', 'æ', 'ø', 'å', 'ä', 'ö', 'ü', 'é'])
@Field final int NGRAM_MODE = 3 // Defaults to Trigram for best name realism
@Field final int MAX_NAME_LEN = 12

// ----------------------------------------------------------------
// --- DATA STRUCTURES ---
// ----------------------------------------------------------------

@Canonical
class WeightedSelector {
    final List<String> keys
    final List<Long> cumulativeWeights
    final long totalWeight

    WeightedSelector(Map<String, Integer> map) {
        this.keys = []
        this.cumulativeWeights = []
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

// A generic linguistic model (Reusable for First, Middle, or Last)
class LanguageModel {
    String name // "FIRST", "MIDDLE", "LAST"
    Map<String, Integer> lengthStats = [:]

    // N-Gram Data
    Map<Integer, Map<String, Map<String, Integer>>> startTokens = [:] // [Len][Type][Char]
    Map<Integer, Map<String, Map<String, Integer>>> innerTokens = [:]
    Map<Integer, Map<String, Map<String, Integer>>> lastTokens = [:]

    Map<Integer, Map<String, Map<String, Integer>>> bigramInner = [:]
    Map<Integer, Map<String, Map<String, Integer>>> trigramInner = [:]
    Map<Integer, Map<String, Map<String, Integer>>> trigramLast = [:]

    // Compiled Selectors
    WeightedSelector lengthSelector
    Map<Integer, Map<String, WeightedSelector>> startSel = [:]
    Map<Integer, Map<String, WeightedSelector>> innerSel = [:]
    Map<Integer, Map<String, WeightedSelector>> lastSel = [:]
    Map<Integer, Map<String, WeightedSelector>> bigramSel = [:]
    Map<Integer, Map<String, WeightedSelector>> trigramInnerSel = [:]
    Map<Integer, Map<String, WeightedSelector>> trigramLastSel = [:]
}

// The Container for the structural rules
class NameManager {
    LanguageModel firstModel = new LanguageModel(name: "FIRST")
    LanguageModel middleModel = new LanguageModel(name: "MIDDLE")
    LanguageModel lastModel = new LanguageModel(name: "LAST")

    // Stores the patterns found (e.g., [FIRST, LAST], [FIRST, FIRST, MIDDLE, LAST])
    List<List<String>> structureTemplates = []
}

// ----------------------------------------------------------------
// --- LOGIC HELPER ---
// ----------------------------------------------------------------

def tokenize(String word) {
    def clean = word.toLowerCase().findAll { Character.isLetter(it as char) }.join('')
    if (!clean) return []

    def tokens = []
    def curType = ''
    def curContent = new StringBuilder()

    clean.each { c ->
        def type = VOWELS.contains(c.toString()) ? 'V' : 'C'
        if (curType == '' || curType == type) {
            curType = type
            curContent.append(c)
        } else {
            tokens << [curType, curContent.toString()]
            curType = type
            curContent = new StringBuilder().append(c)
        }
    }
    if (curContent) tokens << [curType, curContent.toString()]
    return tokens
}

def trainModel(LanguageModel model, String word) {
    def tokens = tokenize(word)
    if (!tokens) return

    def len = Math.min(tokens.size(), MAX_NAME_LEN)
    def startType = tokens[0][0]
    def key = "${len}:${startType}"
    model.lengthStats[key] = (model.lengthStats[key] ?: 0) + 1

    // Basic structural stats
    tokens.eachWithIndex { t, i ->
        def content = t[1]
        if (i == 0) {
            model.startTokens.computeIfAbsent(len, {[:]}).computeIfAbsent(startType, {[:]}).merge(content, 1, Integer::sum)
        } else {
            def prev = tokens[i-1]
            def prevType = prev[0]
            def prevContent = prev[1]
            def map = (i == len - 1) ? model.lastTokens : model.innerTokens
            map.computeIfAbsent(len, {[:]}).computeIfAbsent(prevType, {[:]}).merge(content, 1, Integer::sum)

            // Bigrams
            model.bigramInner.computeIfAbsent(len, {[:]}).computeIfAbsent(prevContent, {[:]}).merge(content, 1, Integer::sum)

            // Trigrams
            if (i >= 2) {
                def prev2Content = tokens[i-2][1]
                def triKey = "${prev2Content}:${prevContent}"
                def triMap = (i == len - 1) ? model.trigramLast : model.trigramInner
                triMap.computeIfAbsent(len, {[:]}).computeIfAbsent(triKey, {[:]}).merge(content, 1, Integer::sum)
            }
        }
    }
}

def compileModel(LanguageModel model) {
    model.lengthSelector = new WeightedSelector(model.lengthStats)

    def build = { src, dest ->
        src.each { k1, inner -> dest[k1] = [:]
            inner.each { k2, map -> dest[k1][k2] = new WeightedSelector(map) }
        }
    }

    build(model.startTokens, model.startSel)
    build(model.innerTokens, model.innerSel)
    build(model.lastTokens, model.lastSel)
    build(model.bigramInner, model.bigramSel)
    build(model.trigramInner, model.trigramInnerSel)
    build(model.trigramLast, model.trigramLastSel)
}

// ----------------------------------------------------------------
// --- ANALYSIS ---
// ----------------------------------------------------------------

def analyzeFile(String path) {
    def manager = new NameManager()
    def text = new File(path).text
    // Split by newline or punctuation to get full names
    def rawNames = text.split(/[.\n!?]+/)

    rawNames.each { rawName ->
        def parts = rawName.trim().split(/\s+/)
        def cleanParts = parts.findAll { it.length() > 0 }
        if (!cleanParts) return

        List<String> currentStructure = []

        // --- RULE IMPLEMENTATION ---

        if (cleanParts.size() == 1) {
            // Rule: One word -> Last Name
            def p = cleanParts[0]
            // Handle hyphens for training, but keep structure simple
            p.split("-").each { trainModel(manager.lastModel, it) }
            currentStructure.add("LAST")
        }
        else if (cleanParts.size() == 2) {
            // Rule: Two words -> First, Last
            cleanParts[0].split("-").each { trainModel(manager.firstModel, it) }
            currentStructure.add("FIRST")

            cleanParts[1].split("-").each { trainModel(manager.lastModel, it) }
            currentStructure.add("LAST")
        }
        else {
            // Rule: 3+ words. Middle Split Logic.
            // First is FIRST. Last is LAST.
            // Remainder are split between FIRST and MIDDLE.

            // 1. Process FIRST
            cleanParts[0].split("-").each { trainModel(manager.firstModel, it) }
            currentStructure.add("FIRST")

            // 2. Process MIDDLE / EXTRA FIRSTS
            def middleTokens = cleanParts[1..-2]
            int totalMiddleTokens = middleTokens.size()

            // Logic: "Where middle gets the remainder".
            // 1 token -> 1 Middle
            // 2 tokens -> 1 First, 1 Middle
            // 3 tokens -> 1 First, 2 Middle
            // Formula: Middle Count = ceil(Total / 2).

            int middleCount = (int) Math.ceil(totalMiddleTokens / 2.0)
            int extraFirstCount = totalMiddleTokens - middleCount

            // Assign Extra Firsts
            for (int i = 0; i < extraFirstCount; i++) {
                middleTokens[i].split("-").each { trainModel(manager.firstModel, it) }
                currentStructure.add("FIRST")
            }

            // Assign Middles
            for (int i = extraFirstCount; i < totalMiddleTokens; i++) {
                middleTokens[i].split("-").each { trainModel(manager.middleModel, it) }
                currentStructure.add("MIDDLE")
            }

            // 3. Process LAST
            cleanParts[-1].split("-").each { trainModel(manager.lastModel, it) }
            currentStructure.add("LAST")
        }

        manager.structureTemplates.add(currentStructure)
    }

    compileModel(manager.firstModel)
    compileModel(manager.middleModel)
    compileModel(manager.lastModel)

    return manager
}

// ----------------------------------------------------------------
// --- GENERATION ---
// ----------------------------------------------------------------

def generateWordPart(LanguageModel model) {
    def fusedKey = model.lengthSelector.select(RND)
    if (!fusedKey) return "Name"

    def parts = fusedKey.split(':')
    def len = parts[0].toInteger()
    def startType = parts[1]

    def tokens = []
    tokens << model.startSel[len][startType].select(RND)
    def prevType = startType

    for (int i = 1; i < len; i++) {
        def isLast = (i == len - 1)
        def token = null

        // Trigram
        if (tokens.size() >= 2) {
            def tgKey = "${tokens[-2]}:${tokens[-1]}"
            def sel = isLast ? model.trigramLastSel : model.trigramInnerSel
            token = sel[len]?[tgKey]?.select(RND)
        }

        // Bigram
        if (!token) {
            def bgKey = tokens[-1]
            def sel = model.bigramSel
            token = sel[len]?[bgKey]?.select(RND)
        }

        // Unigram Fallback
        if (!token) {
            def sel = isLast ? model.lastSel : model.innerSel
            token = sel[len]?[prevType]?.select(RND)
        }

        if (!token) token = (prevType == 'C') ? 'a' : 'b'
        tokens << token
        prevType = (prevType == 'C' ? 'V' : 'C')
    }

    return tokens.join('').capitalize()
}

def generateName(NameManager manager) {
    if (!manager.structureTemplates) return "Error: No Data"

    // Pick a structure (e.g., [FIRST, MIDDLE, LAST])
    def structure = manager.structureTemplates[RND.nextInt(manager.structureTemplates.size())]

    def results = []
    structure.each { type ->
        switch(type) {
            case "FIRST": results << generateWordPart(manager.firstModel); break
            case "MIDDLE": results << generateWordPart(manager.middleModel); break
            case "LAST": results << generateWordPart(manager.lastModel); break
        }
    }
    return results.join(" ")
}


// ----------------------------------------------------------------
// --- MAIN ---
// ----------------------------------------------------------------

def argsMap = [:]
args.eachWithIndex { arg, i ->
    if (arg == '-f') argsMap['file'] = args[i+1]
    if (arg == '-c') argsMap['count'] = args[i+1].toInteger()
}

if (!argsMap['file']) {
    println "Usage: groovy PseudoNames.groovy -f <file> [-c <count>]"
    System.exit(0)
}

int count = argsMap['count'] ?: 20

println "📖 Learning Name Rules from ${argsMap['file']}..."
def manager = analyzeFile(argsMap['file'])

println "📊 Stats:"
println "   First Names patterns: ${manager.firstModel.lengthStats.size()}"
println "   Middle Names patterns: ${manager.middleModel.lengthStats.size()}"
println "   Last Names patterns: ${manager.lastModel.lengthStats.size()}"
println "   Structure Templates: ${manager.structureTemplates.size()}"

println "\n✍️  GENERATED NAMES:"
println "=" * 50
count.times {
    println generateName(manager)
}
println "=" * 50