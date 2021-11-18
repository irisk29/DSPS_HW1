import org.apache.commons.io.FileUtils;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.rendering.ImageType;
import org.apache.pdfbox.rendering.PDFRenderer;
import org.apache.pdfbox.text.PDFTextStripper;
import org.apache.pdfbox.tools.imageio.ImageIOUtil;
import org.fit.pdfdom.PDFDomTree;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;

public class WorkerActions {
    private static String convertPDFToImage(URL url)
    {
        try (InputStream is = url.openStream(); PDDocument document = PDDocument.load(is)) {
            PDFRenderer pdfRenderer = new PDFRenderer(document);
            BufferedImage bim = pdfRenderer.renderImageWithDPI(0, 300, ImageType.RGB);
            File outputFile = new File(url.getFile());
            ImageIOUtil.writeImage(bim, outputFile.getName(), 300);
            document.close();
            return outputFile.getAbsolutePath();
        } catch (IOException exception) {
            System.out.println(exception.getMessage());
            return null;
        }
    }

    private static String convertPDFToTextFile(URL url){
        try (InputStream is = url.openStream(); PDDocument doc = PDDocument.load(is)) {
            String text = new PDFTextStripper().getText(doc);
            File file = new File(url.getFile());
            FileUtils.writeStringToFile(file, text, StandardCharsets.UTF_8);
            return file.getAbsolutePath();
        } catch (IOException exception) {
            System.out.println(exception.getMessage());
            return null;
        }
    }

    private static String convertPDFToHTML(URL url) {
        try (InputStream is = url.openStream(); PDDocument document = PDDocument.load(is)) {
            File outputFile = new File(url.getFile());
            Writer output = new PrintWriter(outputFile.getName(), StandardCharsets.UTF_8);
            new PDFDomTree().writeText(document, output);
            output.close();
            return outputFile.getAbsolutePath();
        } catch (IOException exception) {
            System.out.println(exception.getMessage());
            return null;
        }
    }

    // runs the relevant task according to action
    // returns the result file path
    public static String doWorkerAction(String action, String pdfStringUrl) throws MalformedURLException {
        URL pdfUrl = new URL(pdfStringUrl);
        String resultFilePath = null;
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
