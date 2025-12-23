package org.tdl.ptg.magic

import groovy.json.JsonSlurper

// Check arguments
if (args.length < 2) {
    System.err.println "Usage: groovy fetch_flavor.groovy <input_file> <output_file>"
    System.err.println "  input_file  : File with card names, one per line"
    System.err.println "  output_file : Output file for flavor texts, one per line"
    System.exit(1)
}

def inputFile = new File(args[0])
def outputFile = new File(args[1])

if (!inputFile.exists()) {
    System.err.println "Error: Input file '${args[0]}' not found"
    System.exit(1)
}

def cardNames = inputFile.readLines().findAll { it.trim() }
System.err.println "Processing ${cardNames.size()} cards..."

def jsonSlurper = new JsonSlurper()
def flavorTexts = []
def notFound = []
def noFlavor = []

cardNames.eachWithIndex { cardName, idx ->
    if ((idx + 1) % 10 == 0) {
        System.err.print "\rProcessed ${idx + 1}/${cardNames.size()}..."
    }

    // URL encode the card name
    def encodedName = URLEncoder.encode(cardName.trim(), "UTF-8")
    def url = "https://api.scryfall.com/cards/named?exact=${encodedName}"

    try {
        def connection = new URL(url).openConnection()
        connection.setRequestProperty("User-Agent", "FlavorTextFetcher/1.0")
        connection.setRequestProperty("Accept", "application/json")

        if (connection.responseCode == 200) {
            def response = jsonSlurper.parse(connection.inputStream)
            def flavorText = response.flavor_text ?: ""

            if (flavorText) {
                // Replace newlines with spaces to keep one line per card
                flavorText = flavorText.replaceAll(/\r?\n/, " ").trim()
                flavorTexts << flavorText
            } else {
                noFlavor << cardName
                flavorTexts << ""
            }
        } else {
            notFound << cardName
            flavorTexts << ""
        }
    } catch (Exception e) {
        System.err.println "\nError fetching '${cardName}': ${e.message}"
        notFound << cardName
        flavorTexts << ""
    }

    // Scryfall asks for 50-100ms delay between requests
    Thread.sleep(100)
}

System.err.println "\rProcessed ${cardNames.size()}/${cardNames.size()}... Done!"

// Write output
outputFile.withWriter { w ->
    flavorTexts.each { w.writeLine(it) }
}

System.err.println "Output written to: ${outputFile.name}"
System.err.println "Cards found: ${cardNames.size() - notFound.size()}"
if (notFound) {
    System.err.println "Cards not found (${notFound.size()}): ${notFound.take(10).join(', ')}${notFound.size() > 10 ? '...' : ''}"
}
if (noFlavor) {
    System.err.println "Cards without flavor text (${noFlavor.size()}): ${noFlavor.take(10).join(', ')}${noFlavor.size() > 10 ? '...' : ''}"
}

