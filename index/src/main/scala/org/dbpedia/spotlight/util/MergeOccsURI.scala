package org.dbpedia.spotlight.util

import scala.io.Source
import java.io.{PrintStream, FileWriter}
import org.dbpedia.spotlight.model.{SpotlightConfiguration, SurfaceForm, DBpediaResourceOccurrence}
import scala.collection.mutable

object MergeOccsURI {

  def mergeUsingOccs(relationFile: String, occsFile: String, outputOccsFile: String) {
    val map = Source.fromFile(relationFile).getLines().map(line => {
      (line.split("> <")(2).replace("> .", ""), line.split("> <")(0).replace("<", ""))
    }).toMap

    val buffer = new StringBuilder

    var processed = 0

    Source.fromFile(occsFile).getLines().foreach(line => {
        buffer.append(line.split("\t")(0) + "\t" +
          getURI(map, line.split("\t")(1)) + "\t" +
          line.split("\t")(2) + "\t" +
          line.split("\t")(3) + "\t" +
          (line.split("\t")(line.split("\t").size - 1)).toInt + "\n")

        processed += 1

        if (processed % 1000 == 0) {
          writeToFile(outputOccsFile, buffer.toString())
          buffer.clear()
          println(" %d rows processed".format(processed))
        }
      }
    )

    println("done!")
  }

  def generateRelationHash(config: IndexingConfiguration): mutable.HashMap[String, String] = {
    println("Creating a hash with the relation file to some Ontology...")
    val mapToOtherOntology = config.get("org.dbpedia.spotlight.data.mapToOtherOntology")
    val relationHash = new mutable.HashMap[String, String]()

    for (line <- Source.fromFile(mapToOtherOntology).getLines()) {
      val lineArray = line.split(" ")
      //println("globo = " + lineArray(0).dropRight(1).reverse.dropRight(1).reverse)
      //println("db = " + lineArray(2).dropRight(1).reverse.dropRight(1).reverse)
      try {
        //println("db = " + lineArray(2).dropRight(1).reverse.dropRight(1).reverse)
        //println("glb = " + lineArray(0).dropRight(1).reverse.dropRight(1).reverse)
        relationHash.put(lineArray(2).dropRight(1).reverse.dropRight(1).reverse, lineArray(0).dropRight(1).reverse.dropRight(1).reverse)
      } catch {
        case e: Exception => println("Skipping invalid line!")
      }
    }
    println("Done.")

    relationHash
  }

  def mergeUsingOccSource(occSource: Traversable[DBpediaResourceOccurrence], config: IndexingConfiguration) {

    println("Creating a hash with the relation file to some Ontology...")
    val mapToOtherOntology = config.get("org.dbpedia.spotlight.data.mapToOtherOntology")
    val relationHash = new mutable.HashMap[String, String]()
    for (line <- Source.fromFile(mapToOtherOntology).getLines()) {
      val lineArray = line.split(" ")
      //println("globo = " + lineArray(0).dropRight(1).reverse.dropRight(1).reverse)
      //println("db = " + lineArray(2).dropRight(1).reverse.dropRight(1).reverse)
      try {
        //println("db = " + lineArray(2).dropRight(1).reverse.dropRight(1).reverse)
        //println("glb = " + lineArray(0).dropRight(1).reverse.dropRight(1).reverse)
        relationHash.put(lineArray(2).dropRight(1).reverse.dropRight(1).reverse, lineArray(0).dropRight(1).reverse.dropRight(1).reverse)
      } catch {
        case e: Exception => println("Skipping invalid line!")
      }
    }
    println("Done.")

    println("Changing the surface forms from wikipedia according to the mapping...")
    var i = 1
    for (occ <- occSource.toList) {
      val newUriName = relationHash.getOrElse(occ.resource.uri, "")
      //println("de boa? = " + occ.resource.uri)
      if (newUriName != "") {
        occ.resource.setUri(newUriName)
      }
      if (i % 100000 == 0) println(i + " lines processed...")
      i += 1
    }
    println("Done.")
  }

  def getURI(map: Map[String, String], uri: String): String = {
    map.getOrElse(uri.trim(), uri.trim())
  }

  def writeToFile(p: String, s: String) {
    val fw = new FileWriter(p, true)
    fw.write(s)
    fw.close()

  }

  def main(args : Array[String]) {

    println("Creating a hash with the relation file to some Ontology...")
    val relationHash = new mutable.HashMap[String, String]()
    for (line <- Source.fromFile("E:/globo_resources/labels_pt.nt").getLines()) {
      val lineArray = line.split(">")
      try {
        val currentKey = lineArray(0).reverse.dropRight(1).reverse.split('/').last
        val currentValue = lineArray(2).dropRight(6).reverse.dropRight(2).reverse
        //println(currentKey)
        //println(currentValue)
        relationHash.put(currentKey, currentValue)
      } catch {
        case e: Exception => println("Skipping invalid line!")
      }
    }
    println("Done.")

    println("Replacing surface forms.")
    val sfStream = new PrintStream("E:/NamespacesData/output/pt/newSurfaceForms.tsv", "UTF-8")
    var i = 1
    for (line <- Source.fromFile("E:/NamespacesData/output/pt/surfaceForms-fromTitRedDis.tsv").getLines()) {
      val lineArray = line.split('\t')
      val newSF = relationHash.getOrElse(lineArray(0),"")
      if (!newSF.isEmpty) {
        if (newSF.length <= 50) {
          sfStream.println(newSF.replaceAll("_","") + '\t' + lineArray(1))
        }
      } else {
        sfStream.println(line)
      }
      if (i % 100000 == 0) println (i + " lines processed...")
      i += 1
    }
    println("Done.")
  }
}
