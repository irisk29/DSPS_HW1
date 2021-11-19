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
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;

public class WorkerActions {
    private static Result<String> convertPDFToImage(URL url)
    {
        try (InputStream is = url.openStream(); PDDocument document = PDDocument.load(is)) {
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

    private static Result<String> convertPDFToTextFile(URL url){
        try (InputStream is = url.openStream(); PDDocument doc = PDDocument.load(is)) {
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

    private static Result<String> convertPDFToHTML(URL url) {
        try (InputStream is = url.openStream(); PDDocument document = PDDocument.load(is)) {
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
        URL pdfUrl = new URL(pdfStringUrl);
        Result<String> resultFilePath = null;
        switch (action) {
            case "ToImage":
                resultFilePath = convertPDFToImage(pdfUrl);
                break;
            case "ToHTML":
                resultFilePath = convertPDFToHTML(pdfUrl);
                break;
            case "ToText":
                resultFilePath = convertPDFToTextFile(pdfUrl);
                break;
        }
        return resultFilePath;
    }
}
