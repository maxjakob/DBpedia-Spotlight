package org.dbpedia.spotlight.spot

import fst.FsaSpotter
import org.dbpedia.spotlight.model.{SurfaceFormOccurrence, Text, SurfaceForm, Factory}
import org.scalatest._
import matchers.ShouldMatchers
import scala.collection.JavaConversions._

class SpotterTest extends FlatSpec with ShouldMatchers {

    "FstSpotter" should "find word in first position" in {
        val in = new Text("Words are what we search")
        val expected = List(
            new SurfaceFormOccurrence(Factory.SurfaceForm.fromString("Words"), in, 0)
        )
        getOutput(List("Words"), in) should equal (expected)
    }

    it should "find phrase in first position" in {
        val in = new Text("Memory efficiency is what we search")
        val expected = List(
            new SurfaceFormOccurrence(Factory.SurfaceForm.fromString("Memory efficiency"), in, 0)
        )
        getOutput(List("Memory efficiency"), in) should equal (expected)
    }

    it should "find word in the middle" in {
        val in = new Text("Getting a correct offset is something important")
        val expected = List(
            new SurfaceFormOccurrence(Factory.SurfaceForm.fromString("correct"), in, 10)
        )
        getOutput(List("correct"), in) should equal (expected)
    }


    private def getOutput(surfaceForms: List[String], input: Text) = {
        val sfs = surfaceForms.map(s => Factory.SurfaceForm.fromString(s))
        new FsaSpotter(sfs).extract(input).toList

    }

}

