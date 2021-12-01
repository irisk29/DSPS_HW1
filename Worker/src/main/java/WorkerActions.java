import Result.*;
import org.apache.commons.io.FileUtils;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.rendering.ImageType;
import org.apache.pdfbox.rendering.PDFRenderer;
import org.apache.pdfbox.text.PDFTextStripper;
import org.apache.pdfbox.tools.imageio.ImageIOUtil;
import org.fit.pdfdom.PDFDomTree;

import java.awt.image.BufferedImage;
import java.io.*;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;

public class WorkerActions {
    private static Result<String> convertPDFToImage(HttpURLConnection url)
    {
        try (InputStream is = url.getInputStream(); PDDocument document = PDDocument.load(is)) {
            PDFRenderer pdfRenderer = new PDFRenderer(document);
            BufferedImage bim = pdfRenderer.renderImageWithDPI(0, 300, ImageType.RGB);
            String fileName = "outputFile" + System.currentTimeMillis() + ".png";
            File outputFile = new File(fileName);
            ImageIOUtil.writeImage(bim, outputFile.getName(), 300);
            document.close();
            return new Ok<String>(url.toString(), outputFile.getAbsolutePath());
        } catch (IOException exception) {
            System.out.println(exception.getMessage());
            return new Fail<String>(url.toString(), exception.getMessage());
        }
    }

    private static Result<String> convertPDFToTextFile(HttpURLConnection url){
        try (InputStream is = url.getInputStream(); PDDocument doc = PDDocument.load(is)) {
            String text = new PDFTextStripper().getText(doc);
            String fileName = "outputFile" + System.currentTimeMillis() + ".txt";
            File file = new File(fileName);
            FileUtils.writeStringToFile(file, text, StandardCharsets.UTF_8);
            return new Ok<String>(url.toString(), file.getAbsolutePath());
        } catch (IOException exception) {
            System.out.println(exception.getMessage());
            return new Fail<String>(url.toString(), exception.getMessage());
        }
    }

    private static Result<String> convertPDFToHTML(HttpURLConnection url) {
        try (InputStream is = url.getInputStream(); PDDocument document = PDDocument.load(is)) {
            String fileName = "outputFile" + System.currentTimeMillis() + ".html";
            File outputFile = new File(fileName);
            Writer output = new PrintWriter(outputFile.getName(), StandardCharsets.UTF_8);
            new PDFDomTree().writeText(document, output);
            output.close();
            return new Ok<String>(url.toString(), outputFile.getAbsolutePath());
        } catch (IOException exception) {
            System.out.println(exception.getMessage());
            return new Fail<String>(url.toString(), exception.getMessage());
        }
    }

    // runs the relevant task according to action
    // returns the result file path
    public static Result<String> doWorkerAction(String action, String pdfStringUrl) throws MalformedURLException {
        Result<String> resultFilePath = null;
        try {
            URL pdfUrl = new URL(pdfStringUrl);
            HttpURLConnection huc = (HttpURLConnection) pdfUrl.openConnection();
            huc.setConnectTimeout(15 * 1000); //15 seconds until we get the timeout from the url
            switch (action) {
                case "ToImage" -> resultFilePath = convertPDFToImage(huc);
                case "ToHTML" -> resultFilePath = convertPDFToHTML(huc);
                case "ToText" -> resultFilePath = convertPDFToTextFile(huc);
            }
            return resultFilePath;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return resultFilePath;
    }
}
