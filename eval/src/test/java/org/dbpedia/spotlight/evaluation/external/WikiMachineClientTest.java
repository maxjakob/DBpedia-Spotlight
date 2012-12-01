/**
 * Copyright 2011 Pablo Mendes, Max Jakob, Joachim Daiber
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.dbpedia.spotlight.evaluation.external;

import junit.framework.TestCase;
import org.dbpedia.spotlight.model.DBpediaResource;
import org.dbpedia.spotlight.model.Factory;
import org.dbpedia.spotlight.model.Text;
import org.junit.Test;

import java.net.URLEncoder;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * ExtractivClient test case.
 * TODO set up one test class for AnnotationClient that contains methods for each of the clients in order to avoid boilerplate code
 *
 * @author pablomendes, adapted from jodaiber
 * 
 */
public class WikiMachineClientTest extends TestCase {

	private AnnotationClient client;
	private Text text;

    public void setUp() throws Exception {
        super.setUp();
		client = new WikiMachineClient();
		
		text = new Text("Google Inc. is an American multinational public corporation " +
				"invested in Internet search, cloud computing, and advertising " +
				"technologies. Google hosts and develops a number of Internet-based " +
				"services and products, and generates profit primarily from advertising " +
				"through its AdWords program.");
    }

    public void testExtract() throws Exception {
    	assertNotNull(client.extract(text));
    }

    public void testExtractReturnsFilledList() throws Exception {
    	assertNotSame(0, client.extract(text).size());
    }

    @Test
    public void test() throws Exception {

        //String api_key = args[0];
        WikiMachineClient c = new WikiMachineClient();

        String text ="The Empire of Brazil was a 19th-century nation that broadly comprised the territories which form modern Brazil. "
                + "Its government was a representative parliamentary constitutional monarchy under the rule of Emperors Dom Pedro I "
                + "and his son Dom Pedro II. On 7 September 1822, Pedro declared the independence of Brazil and, after waging a "
                + "successful war against his father's kingdom, was acclaimed on 12 October as Pedro I, the first Emperor of Brazil. ";

        text= URLEncoder.encode(text, "utf-8");

        List<DBpediaResource> response = c.extract(new Text(text));

        DBpediaResource[] expectedEntities = {Factory.getDBpediaResource().from("Son"), Factory.getDBpediaResource().from("Pedro_I_of_Brazil"), Factory.getDBpediaResource().from("Territory_%28administrative_division%29"), Factory.getDBpediaResource().from("Brazil"), Factory.getDBpediaResource().from("Monarchy"), Factory.getDBpediaResource().from("Father"), Factory.getDBpediaResource().from("Governance"), Factory.getDBpediaResource().from("Pedro_I_of_Brazil"), Factory.getDBpediaResource().from("Wars_of_succession"), Factory.getDBpediaResource().from("Empire_of_Brazil"), Factory.getDBpediaResource().from("Pedro_II_of_Brazil"), Factory.getDBpediaResource().from("Brazilian_Declaration_of_Independence"), Factory.getDBpediaResource().from("Politics_of_the_Empire_of_Brazil"), Factory.getDBpediaResource().from("Constitutional_monarchy"), Factory.getDBpediaResource().from("October"), Factory.getDBpediaResource().from("September"), Factory.getDBpediaResource().from("Emperor"), Factory.getDBpediaResource().from("Nation"), Factory.getDBpediaResource().from("Government")};
        //TODO sort and compare
        assertEquals(Arrays.asList(expectedEntities).size(),response.size());

    }
  
}
