#!/usr/bin/env groovy

import groovy.json.JsonSlurper
import groovy.json.JsonOutput
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.net.URI

/**
 * Fetches Norwegian boys' names from Statistics Norway (SSB) Statbank API
 *
 * Uses table 10501: "Persons, by girls' name and boys' name" which contains
 * all persons registered in Norway by first name.
 *
 * API Documentation: https://www.ssb.no/en/api/pxwebapi
 */

class SSBNameFetcher {

    // Using table 10467: "Born persons, by girls' name and boys' name"
    // This table has births separated by gender, unlike 10501
    static final String BASE_URL = "https://data.ssb.no/api/v0/en/table/10467"
    static final HttpClient client = HttpClient.newHttpClient()

    /**
     * Fetch metadata for the table to understand structure
     */
    static Map fetchMetadata() {
        println "📊 Fetching table metadata..."

        def request = HttpRequest.newBuilder()
                .uri(URI.create(BASE_URL))
                .header("Accept", "application/json")
                .GET()
                .build()

        def response = client.send(request, HttpResponse.BodyHandlers.ofString())

        if (response.statusCode() != 200) {
            throw new RuntimeException("Failed to fetch metadata: ${response.statusCode()}")
        }

        def metadata = new JsonSlurper().parseText(response.body())

        // Print available variables for debugging
        println "   Available variables:"
        metadata.variables.each { v ->
            println "     - ${v.code}: ${v.text} (${v.values?.size() ?: 0} values)"

            // Show details for key variables
            if (v.code == "ContentsCode" || v.code == "Kjonn") {
                println "       Values:"
                v.values.eachWithIndex { val, idx ->
                    println "         ${idx}: ${val} = ${v.valueTexts[idx]}"
                }
            }
        }

        return metadata
    }

    /**
     * Build a query to fetch ALL names data for 2024
     * We'll filter boys vs girls from the results
     */
    static Map buildNamesQuery() {
        return [
                query: [
                        [
                                code: "Fornavn",
                                selection: [
                                        filter: "all",
                                        values: ["*"]  // All names
                                ]
                        ],
                        [
                                code: "ContentsCode",
                                selection: [
                                        filter: "item",
                                        values: ["Personer"]  // Born persons (count)
                                ]
                        ],
                        [
                                code: "Tid",
                                selection: [
                                        filter: "item",
                                        values: ["2024"]  // Latest year
                                ]
                        ]
                ],
                response: [
                        format: "json-stat2"
                ]
        ]
    }

    /**
     * Fetch names data from SSB with retry logic
     */
    static Map fetchNamesData(int maxRetries = 3) {
        println "🔍 Fetching names data from SSB..."

        def query = buildNamesQuery()
        def queryJson = JsonOutput.toJson(query)

        println "   Query: ${queryJson}"

        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                def request = HttpRequest.newBuilder()
                        .uri(URI.create(BASE_URL))
                        .header("Content-Type", "application/json")
                        .header("Accept", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(queryJson))
                        .build()

                def response = client.send(request, HttpResponse.BodyHandlers.ofString())

                if (response.statusCode() == 200) {
                    return new JsonSlurper().parseText(response.body())
                } else if (response.statusCode() == 503) {
                    println "   ⚠️  Service unavailable (attempt ${attempt}/${maxRetries})"
                    if (attempt < maxRetries) {
                        println "   Waiting 5 seconds before retry..."
                        Thread.sleep(5000)
                        continue
                    }
                }

                throw new RuntimeException("Failed to fetch data: ${response.statusCode()}\n${response.body()}")

            } catch (Exception e) {
                if (attempt == maxRetries) {
                    throw e
                }
                println "   ⚠️  Error on attempt ${attempt}: ${e.message}"
                println "   Waiting 5 seconds before retry..."
                Thread.sleep(5000)
            }
        }

        throw new RuntimeException("Failed after ${maxRetries} attempts")
    }

    /**
     * Parse JSON-stat2 format and extract name/count pairs
     * Returns all names with counts
     */
    static List<Map> parseNamesFromJsonStat(Map data, boolean boysOnly = true) {
        println "📝 Parsing name data..."

        // JSON-stat2 structure
        def names = data.dimension.Fornavn.category.label
        def values = data.value

        println "   Total names found: ${names.size()}"

        if (!boysOnly) {
            println "   Sample names (first 20):"
            names.take(20).eachWithIndex { entry, i ->
                def code = entry.key
                def name = entry.value
                def idx = data.dimension.Fornavn.category.index[code]
                def count = values[idx] ?: 0
                println "     ${i+1}. ${code}: ${name} = ${count}"
            }
        }

        // Create list of [name: count] maps
        // Filter based on code prefix: '1' = girls, '2' = boys
        def nameList = []
        names.each { code, name ->
            def idx = data.dimension.Fornavn.category.index[code]
            def count = values[idx] ?: 0

            // Filter for boys' names (code starts with '2')
            def includeThis = boysOnly ? code.startsWith("2") : true

            if (count > 0 && includeThis) {
                nameList << [name: name, code: code, count: count as Integer]
            }
        }

        def genderLabel = boysOnly ? "Boys'" : "All"
        println "   ${genderLabel} names with counts > 0: ${nameList.size()}"

        return nameList
    }

    /**
     * Sort by count descending and take top N
     */
    static List<Map> getTopNames(List<Map> names, int limit = 1000) {
        return names
                .sort { -it.count }  // Sort descending by count
                .take(limit)         // Take top N
    }

    /**
     * Save names to file
     */
    static void saveToFile(List<Map> names, String filename) {
        println "💾 Saving to ${filename}..."

        new File(filename).withWriter('UTF-8') { writer ->
            writer.println("rank,name,count")
            names.eachWithIndex { nameData, idx ->
                writer.println("${idx + 1},${nameData.name},${nameData.count}")
            }
        }

        println "✅ Saved ${names.size()} names to ${filename}"
    }

    /**
     * Display top 20 names for verification
     */
    static void displayTop20(List<Map> names) {
        println "\n📋 Top 20 Norwegian Boys' Names:"
        println "=" * 50
        names.take(20).eachWithIndex { nameData, idx ->
            def rank = (idx + 1).toString().padLeft(3)
            def name = nameData.name.padRight(20)
            def count = nameData.count.toString().padLeft(8)
            println "${rank}. ${name} ${count} persons"
        }
        println "=" * 50
    }
}

// ================================================================
// MAIN EXECUTION
// ================================================================

println """
╔════════════════════════════════════════════════════════════╗
║  SSB Norwegian Boys' Names Fetcher                         ║
║  Retrieving data from Statistics Norway (SSB)             ║
╚════════════════════════════════════════════════════════════╝
"""

try {
    // Parse command line arguments
    def limit = 1000
    def outputFile = "norwegian_boys_names_top1000.csv"
    def checkMetadata = false

    if (args.length > 0 && args[0] == "--metadata") {
        checkMetadata = true
    } else {
        if (args.length > 0) {
            limit = args[0].toInteger()
        }
        if (args.length > 1) {
            outputFile = args[1]
        }
    }

    if (checkMetadata) {
        println "Checking table metadata..."
        def metadata = SSBNameFetcher.fetchMetadata()
        println "\n✅ Metadata retrieved successfully"
        System.exit(0)
    }

    println "Configuration:"
    println "  → Limit: ${limit} names"
    println "  → Output: ${outputFile}"
    println ""

    // Fetch and process data
    def data = SSBNameFetcher.fetchNamesData()
    def namesList = SSBNameFetcher.parseNamesFromJsonStat(data)
    def topNames = SSBNameFetcher.getTopNames(namesList, limit)

    // Display results
    SSBNameFetcher.displayTop20(topNames)

    // Save to file
    SSBNameFetcher.saveToFile(topNames, outputFile)

    println "\n✨ Success! Retrieved ${topNames.size()} Norwegian boys' names."
    println "📊 Total unique names in database: ${namesList.size()}"

} catch (Exception e) {
    println "\n❌ Error: ${e.message}"
    e.printStackTrace()
    System.exit(1)
}