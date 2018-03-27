/**
 * Created by wliu on 11/14/17.
 */
package com.intrence.cdg.extractor;

import com.intrence.cdg.parser.TestUtil;
import org.htmlcleaner.TagNode;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class HtmlExtractorTest {

    private static final String SOURCE = "new_merchant_parser";
    private HtmlExtractor htmlExtractor;

    @Before
    public void setup() throws Exception{
        htmlExtractor = new HtmlExtractor(SOURCE, TestUtil.getTestHtmlResponse(SOURCE), "http://dummy-url");
    }

    @Test
    public void testHTMLExtractorMethods() {

        String expectedField = "Gramercy Tavern";
        String expectedFieldOne = "American";
        String expectedFieldTwo = "Best Food in New York City";
        String expectedConcatenatedFields = "American,Best Food in New York City";

        TagNode domTree = htmlExtractor.getContentTree();
        String extractedField = htmlExtractor.extractField(
                "//div[@class='heading_name_wrapper']/h1[@id='HEADING']",
                domTree
        );
        String extractedConcatenatedFields = htmlExtractor.extractAndConcatenateFields(
                "//div[@class='heading_details']/div[@class='detail separator']/a",
                domTree
        );
        List<String> extractedFields = htmlExtractor.extractFields(
                "//div[@class='heading_details']/div[@class='detail separator']/a",
                domTree
        );

        assertEquals(expectedField, extractedField);
        assertEquals(expectedConcatenatedFields, extractedConcatenatedFields);
        assertTrue(extractedFields.size() == 2);
        assertTrue(extractedFields.contains(expectedFieldOne));
        assertTrue(extractedFields.contains(expectedFieldTwo));

    }

    @Test(expected = IllegalArgumentException.class)
    public void nullContentTest() {
        new HtmlExtractor(SOURCE, null, "http://dummy-url");
    }

    @Test(expected = IllegalArgumentException.class)
    public void emptyContentTest() {
        new HtmlExtractor(SOURCE, "", "http://dummy-url");
    }

    @Test
    public void simpleHtmlExtractorTest() throws Exception {

        String source = "farfetch";
        String html = TestUtil.getTestHtmlResponse(source);
        String url = "www.farfetch.com";

        HtmlExtractor simpleHtmlExtractor = new HtmlExtractor(source, html, url);
        TagNode domTree = simpleHtmlExtractor.getContentTree();

        String itemTitle = simpleHtmlExtractor.extractField(
                "//div[@class='farfetch-div']/p",
                domTree
        );
        System.out.println(itemTitle);
    }
}
