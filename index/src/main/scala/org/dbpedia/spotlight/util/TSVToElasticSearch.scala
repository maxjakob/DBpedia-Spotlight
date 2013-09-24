package org.dbpedia.spotlight.util

import scala.util.matching.Regex
import java.io.{FileWriter, PrintWriter, FileInputStream, File}
import java.util.Scanner

/**
 * Prototypical transformation script for pignlproc output to ElasticSearch
 * bulk input JSONs. This should probably be transformed into a Pig UDF or
 * at least a Scala script, so that WikipediaToDBpediaClosure can be used.
 * In the Pig case, there is the possibility of joining "tokenCounts",
 * "pairCounts" and "uriCounts" together and produce even more compact
 * bulk indexing operations with multiple fields.

 * See also: ### Bulk import doc page.
 * The _id of the documents is the URI.

 * For now, this does not include custom routing. The initial idea was to
 * spread the documents in a way, that is is unlikely for two concepts that
 * share a surface form to end up in the same shard. This might have the
 * advantage that work can be parallalized and work in the individual shards
 * is minimized. However, it would complicate the indexing process, since
 * we need to keep a mapping of surface forms to all shards that they have
 * been sent to. ElasticSearch uses the has of the ID plus a random sequence
 * to determine the shard, which should already give a good distribution.
 *
 * @author maxjakob
 * zaknarfen (translation to scala)
 */

object TSVToElasticSearch {

  val BULK_SIZE = 5000

  val TYPE_NAME = "entities"
  val SURFACE_FORM = "sf"
  val CONTEXT_FIELD = "context"
  val URI_COUNT = "uriCount"

  // Utility function to append the final string to the initial instance types triples file
  def using[A <: {def close(): Unit}, B](param: A)(f: A => B): B = {
    try { f(param) } finally { param.close() }
  }

  def appendToFile(fileName:String, textData:String) = {
    using (new FileWriter(fileName, true)){
      fileWriter => using (new PrintWriter(fileWriter)) {
        printWriter => printWriter.println(textData)
      }
    }
  }

  def onlyUriEnding(wikiUrl: String): String = {
    //TODO: use WikipediaToDBpediaClosure.scala

    val pattern = new Regex("""(\w*)/wiki/(\w*)""", "firstName", "lastName")
    var result = ""

    try {
      result = pattern.findFirstMatchIn(wikiUrl).get.group("lastName")
    } catch {
      case e: Exception => {
        println("Warning! no URI found in the string " + wikiUrl) // In case a string not containing a /wiki/ is passed
      }
    }
    result
  }

  def encode(aString: String): String = {
    aString.replaceAll("""\\""", """\\\\""").replaceAll(""""""", """\\"""")
  }

  def parseTokenCounts(aLine: String): (String, String, String) = {
    def toTuple(aString: String): (String, String) = {
      val i = aString.indexOf(",")
      new Tuple2(aString.substring(0, i-1), aString.substring(i+1, aString.length-1))
    }

    val line = aLine.split('\t')
    val wikiUrl = line(0)
    val data = line(1)
    println(wikiUrl)
    println(data)
    val uri = onlyUriEnding(wikiUrl)
    var freqs = collection.mutable.Seq[String]() //immutable.Map.empty

    if (data == "{}") {
      freqs :+ "[]"
    } else {
      val stripped = data.substring(2, data.length - 2) //remove starting '{(' and ending ')}'
      println(stripped)
      freqs = freqs ++ stripped.split("""\)"""+','+"""\(""")
      freqs.map( x => toTuple(x))
    }
    val context = freqs.map( f => {
      val strArray = f.split(',')
      ((strArray(0)+" ") * strArray(1).toInt) + " "
    } )
    var contextString = ""
    for (member <- context) {
      contextString += member
    }
    contextString = contextString.substring(0, contextString.length-1)
    Tuple3(uri, CONTEXT_FIELD, '\"'+encode(contextString)+'\"')
  }

  def parsePairCounts(aLine: String): (String, String, String) = {
    //val sf, wikiUrl, freq = aLine.trim().split('\t').toString //freq never used?
    val line = aLine.split('\t')
    val sf = line(0)
    val wikiUrl = line(1)
    val uri = onlyUriEnding(wikiUrl)
    Tuple3(uri, SURFACE_FORM, '\"'+encode(sf)+'\"')
  }

  def parseUriCounts(aLine: String): (String, String, String) = {
    val wikiUrl, freq = aLine.trim().split('\t').toString //freq never used
    val uri = onlyUriEnding(wikiUrl)
    (uri, URI_COUNT, freq)
  }

  def parseSfCounts(aLine: String): (String, String, String) = {
    val line = aLine.split('\t')
    val uri = line(1)
    val sf = line(2)

    Tuple3(uri, SURFACE_FORM, '\"'+encode(sf)+'\"')
  }

  def toJson(indexName: String, typeName: String, uri: String, fieldName: String, value: String, operation: String): String = {
    val indexLine = """{""""+operation+"""":{"_index":""""+indexName+"""","_type":""""+typeName+"""","_id":""""+uri+""""}}"""
    var fieldsLine = ""

    if (operation == "index")
      fieldsLine = """{""""+fieldName+"""":"""+value+"""}"""
    else
      fieldsLine = """{"doc":{""""+fieldName+"""":"""+value+"""}}"""
    indexLine + "\n" + fieldsLine  // + "\n" -> the appendToFile already breaks a line when appending
  }

  def tsvToJSON(aTSVFile: File, outputPath: String) {

    //TODO: end testing all the principal functions, still need to test parseUriCounts
    // Small test of parsePairCounts
    //val currentTuple = parsePairCounts("%\thttp://pt.wikipedia.org/wiki/Porcentagem\t1")

    // Small test of parseTokenCounts
    //val currentTuple = parseTokenCounts("http://pt.wikipedia.org/wiki/1\t{(de,314),(a,189),(1,115),(ano,59),(é,43),(gol,40),(primeir,36),(2,29),(corinthiam,29),(album,27),(dia,27),(3,22),(temp,22),(jogo,19),(tima,19),(outr,16),(sobr,16),(partid,16),(5,14),(vitori,14),(americ,14),(the,14),(0,13),(segund,13),(a.c,13),(apen,13),(sala,12),(seri,12),(time,12),(minut,12),(numer,12),(contr,11),(campeonat,11),(final,11),(paul,11),(lancad,10),(maio,10),(melhor,10),(8,9),(marcou,9),(vide,9),(latin,9),(d.c,9),(calendari,9),(aind,9),(roman,9),(nbsp,9),(doi,9),(7,8),(6,8),(9,8),(leao,8),(laboratori,8),(seguint,8),(parqu,8),(equip,8),(ter,8),(zero,7),(camp,7),(estreou,7),(estadi,7),(setembr,7),(histori,7),(crista,7),(10,7),(crist,7),(film,7),(janeir,7),(conhecid,7),(internacional,7),(2010,7),(of,7),(antartic,7),(4,6),(terceir,6),(comecou,6),(marc,6),(futebol,6),(18,6),(paulist,6),(titul,6),(marcari,6),(animal,6),(estrei,6),(episodi,6),(cidad,6),(copi,6),(amig,6),(milha,6),(1914,6),(lado,6),(fez,5),(medi,5),(cada,5),(fevereir,5),(entant,5),(sob,5),(feit,5),(asterix,5),(maior,5),(poet,5),(24,5),(inteir,5),(institut,5),(luke,5),(vez,5),(band,5),(outubr,5),(fari,5),(tard,5),(ganhou,5),(rogeri,5),(chegou,5),(alvinegr,5),(seman,5),(cont,5),(estad,5),(pul,5),(usad,5),(julh,5),(junh,5),(2009,5),(agost,5),(felicidad,5),(leandr,5),(tre,5),(terminou,5),(armeni,5),(fabinh,5),(padr,5),(deivid,4),(miguelinh,4),(i,4),(rei,4),(faz,4),(toda,4),(todo,4),(participou,4),(direit,4),(brasileir,4),(nort,4),(comum,4),(coordenaca,4),(linh,4),(diretori,4),(gil,4),(itali,4),(precisou,4),(ficou,4),(ultim,4),(ser,4),(sei,4),(faturar,4),(futsal,4),(30,4),(19,4),(15,4),(12,4),(utilizad,4),(sofreri,4),(osiril,4),(menin,4),(secretari,4),(novembr,4),(anno,4),(arrasador,4),(aula,4),(bibliotec,4),(sido,4),(jogou,4),(music,4),(bom,4),(historic,4),(quimic,4),(entram,4),(domini,4),(lingu,4),(belletti,4),(informatic,4),(usa,4),(julian,4),(aconteceu,4),(obelix,4),(imperi,4),(extint,4),(empatar,4),(dirigid,4),(cantor,4),(competica,4),(populaca,4),(dr,4),(corintian,4),(tristez,4),(perdend,4),(secul,4),(inacreditavel,4),(tōkaidō,4),(etap,4),(part,4),(porem,4),(dua,4),(ceni,4),(venci,4),(fort,4),(record,4),(pais,4),(carreir,4),(naquel,4),(escrit,4),(nasciment,4),(durant,4),(entrou,4),(ressurreica,3),(greg,3),(randall,3),(rap,3),(meno,3),(europeu,3),(club,3),(mesm,3),(prat,3),(vali,3),(rio,3),(mund,3),(marcad,3),(cruzeir,3),(personagem,3),(ajudar,3),(viva,3),(germani,3),(mundial,3),(nelsinh,3),(jesu,3),(mand,3),(mant,3),(mina,3),(italian,3),(estaca,3),(mineir,3),(onde,3),(sant,3),(chegand,3),(aldei,3),(beltronens,3),(regia,3),(arbitr,3),(38,3),(descalz,3),(arte,3),(realizari,3),(gregorian,3),(pequen,3),(filh,3),(atual,3),(imediat,3),(grand,3),(amistos,3),(passou,3),(cultur,3),(vendeu,3),(palmeir,3),(ascendent,3),(lembr,3),(fase,3),(kesh,3),(hoje,3),(atuand,3),(bas,3),(palestr,3),(reinad,3),(hit,3),(geral,3),(pie,3),(unic,3),(unia,3),(unid,3),(poi,3),(dioclecian,3),(complet,3),(venceri,3),(cor,3),(resultad,3),(aconteceram,3),(period,3),(tupazinh,3),(antig,3),(canca,3),(junt,3),(poloni,3),(2007,3),(2012,3),(atla,3),(venceu,3),(2000,3),(long,3),(billboard,3),(sistem,3),(lapa,3),(livr,3),(sucedid,3),(mort,3),(1980,3),(golead,3),(data,3),(dest,3),(imperador,3),(top,3),(form,3),(deu,3),(1928,3),(fundad,3),(martin,3),(perdeu,3),(ordem,3),(epoc,3),(exempl,3),(guerr,2),(assemelh,2),(contem,2),(enta,2),(seguid,2),(caca,2),(comercial,2),(dentr,2),(luca,2),(conquistar,2),(jaus,2),(andrew,2),(roma,2),(branda,2),(tornou,2),(virad,2),(decad,2),(assim,2),(nome,2),(diss,2),(jogar,2),(simbol,2),(administraca,2),(marcar,2),(cereal,2),(ouro,2),(querer,2),(escritor,2),(ocident,2),(futebolist,2),(eminem,2),(casa,2),(profetic,2),(vera,2),(proxim,2),(caus,2),(milh,2),(exclusiv,2),(performanc,2),(line,2),(skih,2),(gaules,2),(tornei,2),(august,2),(hanson,2),(decimal,2),(televisa,2),(liaisom,2),(comparad,2),(dezembr,2),(mudanc,2),(encontravam,2),(faturou,2),(banid,2),(aren,2),(artist,2),(vida,2),(trazid,2),(sen,2),(weekend,2),(dupl,2),(hammer,2),(linguagem,2),(sucess,2),(jogand,2),(piracicab,2),(colombi,2),(pode,2),(35,2),(26,2),(27,2),(grup,2),(17,2),(13,2),(21,2),(predecessor,2),(20,2),(edica,2),(cannibal,2),(asiatic,2),(cienag,2),(well,2),(temporad,2),(guitarr,2),(empat,2),(clip,2),(tai,2),(tal,2),(marcand,2),(calcul,2),(acabou,2),(bernie',2),(100,2),(clark,2),(mulher,2),(considerad,2),(infinit,2),(figur,2),(ston,2),(tigran,2),(latim,2),(faleceu,2),(tend,2),(manipular,2),(alem,2),(artig,2),(rival,2),(passad,2),(iniciou,2),(propri,2),(basead,2),(maxim,2),(vendid,2),(europ,2),(ovidi,2),(pilot,2),(paz,2),(abril,2),(oficial,2),(orient,2),(entrevist,2),(gladiador,2),(caetan,2),(frequent,2),(compositor,2),(turqui,2),(apareceu,2),(subespeci,2),(realizad,2),(participaca,2),(pop,2),(certificaca,2),(e.u,2),(los,2),(critic,2),(parad,2),(extinca,2),(american,2),(comu,2),(casad,2),(download,2),(morreu,2),(economic,2),(fenomen,2),(volt,2),(endel,2),(pouc,2),(diamant,2),(baix,2),(modern,2),(romanc,2),(blanc,2),(referid,2),(contagem,2),(cresceu,2),(existiu,2),(traz,2),(formad,2),(pont,2),(2008,2),(alemanh,2),(mong,2),(principal,2),(2011,2),(dionisi,2),(200,2),(afetad,2),(magic,2),(leit,2),(max,2),(precis,2),(tecnic,2),(coloc,2),(at,2),(sabe,2),(iii,2),(produca,2),(amor,2),(peca,2),(nascid,2),(erram,2),(matematic,2),(compar,2),(grachi,2),(amizad,2),(devid,2),(list,2),(chamad,2),(guia,2),(disputou,2),(papa,2),(1990,2),(1996,2),(1997,2),(têm,2),(derrot,2),(crogundi,2),(amar,2),(maiori,2),(administrativ,2),(1909,2),(porqu,2),(1919,2),(nove,2),(apo,2),(instalad,2),(1923,2),(aproximad,2),(novorizontin,2),(1939,2),(show,2),(suspiri,2),(1948,2),(havi,2),(1952,2),(responsavel,2)}")

    //val json = toJson("_pt", TYPE_NAME, currentTuple._1, currentTuple._2, currentTuple._3, "update")
    //println(json)

    // Current Surface Form processing
    val tsvScanner = new Scanner(new FileInputStream(aTSVFile), "UTF-8")

    while(tsvScanner.hasNextLine) {
      val line = tsvScanner.nextLine()
      println(line)

      // The occs.tsv format is:
      // <OccId, URI, SurfaceForm, Text, Offset>
      val currentTuple = parseSfCounts(line)
      val json = toJson("_pt", TYPE_NAME, currentTuple._1, currentTuple._2, currentTuple._3, "update")
      println(json)

      //TODO: Insert bulk size control here

      // Saving json output file
      //TODO: Need to remove the file every time, or it will append to existing ones
      appendToFile(outputPath, json)
    }
  }

  def main(args : Array[String])
  {
    val indexingConfigFileName = args(0)
    val inputFileName = args(1)
    val targetFileName = args(2)

    val config = new IndexingConfiguration(indexingConfigFileName)
    val anOutputPath = config.get("org.dbpedia.spotlight.data.outputBaseDir","")
    val mainLanguage = config.get("org.dbpedia.spotlight.language_i18n_code", "")

    // Parameters i am using at the moment:
    // 1) path/to/output/language/test_100_uri_occs.tsv
    // 2) path/to/output/language/sfCounts
    tsvToJSON(new File(anOutputPath + mainLanguage + '/' + inputFileName), anOutputPath + mainLanguage + '/' + targetFileName)
  }
}
