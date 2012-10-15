package org.dbpedia.spotlight.spot

import org.scalatest.FlatSpec
import org.scalatest.matchers.ShouldMatchers
import org.dbpedia.spotlight.spot.ahocorasick.AhoCorasickSpotter
import io.Source
import org.dbpedia.spotlight.model.{SurfaceFormOccurrence, Text}


class AhoCorasickSpotterTest extends FlatSpec with ShouldMatchers {

  //Config parameters
  //====================================================================================
  // Surface forms format:
  // Extract from tsv file using the following script
  //cat surfaceForms.tsv | cut -d$'\t' -f 1 | sort -u > surfaceForms.set
  val fileName = "/media/SAMSUNG/dbpedia_data/data/output/surfaceForms.set"
  val caseSensitive = false;
  val overlap = false;
  //====================================================================================

  //val classLoader = getClass().getClassLoader();
  //val fileInputStream = classLoader.getResourceAsStream(fileName);
  //val sourceChunks = Source.fromInputStream(fileInputStream)

  val sourceChunks = Source.fromFile(fileName)

  if (sourceChunks == null)
    println("\n * Resource file with the name " + fileName + " was not found.Please,  check it and try again * \n")

  var masterTest = AhoCorasickSpotter.fromSurfaceForms(sourceChunks.getLines(), caseSensitive, overlap)
  sourceChunks.close()

  var text = "What's Up? is a rock song written by Linda Perry for 4 Non Blondes' debut album Bigger, Better, Faster, More! (1992)"
  val surfaceFormsList = List("What's Up?", "is", "a", "rock", "song", "by", "Linda Perry", "4 Non Blondes", "debut", "album", "Bigger, Better, Faster, More!")
  println(" testSurfaceForms --- [BOF]")
  println("Text:")
  println(text)
  println("\n")
  printResults(masterTest.extract(new Text(text)));

  text = "O ex-presidente do PT José Genoino entregou o cargo de assessor especial do Ministério da Defesa nesta quarta-feira (10)"
  println(" testSurfaceForms --- [BOF]")
  println("Text:")
  println(text)
  println("\n")
  printResults(masterTest.extract(new Text(text)))
  println(" testSurfaceForms --- [EOF]\n\n")



  text = "O Aeroporto Internacional Salgado Filho, em Porto Alegre (RS), passou cerca de três horas fechado para pousos e decolagens na noite desta quarta-feira (10) depois que um jato saiu da pista durante o pouso"
  println(" testSurfaceForms --- [BOF]")
  println("Text:")
  println(text)
  println("\n")
  printResults(masterTest.extract(new Text(text)))
  println(" testSurfaceForms --- [EOF]\n\n")


  def printResults(results: java.util.List[SurfaceFormOccurrence]) {

    var iter = results.iterator
    while (iter.hasNext) {
      println((iter.next))
    }
  }

}
