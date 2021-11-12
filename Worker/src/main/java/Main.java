import java.awt.image.BufferedImage;
import java.io.*;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.rendering.ImageType;
import org.apache.pdfbox.rendering.PDFRenderer;
import org.apache.pdfbox.text.PDFTextStripper;
import org.apache.pdfbox.tools.imageio.ImageIOUtil;
import org.apache.commons.io.FileUtils;
import org.fit.pdfdom.PDFDomTree;

public class Main {

    public static void convertPDFToImage()
    {
        //read from SQS1 the operation and the link to the pdf file
        try
        {
            var pdfFilename = "picture.pdf";
            var file = new File("C:\\Users\\irisk\\OneDrive\\Documents\\university\\fourth year - first semester\\DSPS\\hw1\\src\\main\\java\\picture.pdf");
            PDDocument document = PDDocument.load(file);
            PDFRenderer pdfRenderer = new PDFRenderer(document);
            BufferedImage bim = pdfRenderer.renderImageWithDPI(0, 300, ImageType.RGB);
            // suffix in filename will be used as the file format
            ImageIOUtil.writeImage(bim, pdfFilename + "-" + 0 + ".png", 300);
            document.close();
        }catch (IOException exception)
        {
            System.out.println(exception.getMessage());
        }
    }

    public static void ConvertPDFToTextFile(){
        try (InputStream is = new URL("http://www.bethelnewton.org/images/Passover_Guide_BOOKLET.pdf").openStream(); PDDocument doc = PDDocument.load(is)) {
        /*var file = new File("C:\\Users\\irisk\\OneDrive\\Documents\\university\\fourth year - first semester\\DSPS\\hw1\\src\\main\\java\\try.pdf");
        PDDocument doc = PDDocument.load(file);
        String text = new PDFTextStripper().getText(doc);
        FileUtils.writeStringToFile(new File("test.txt"), text, StandardCharsets.UTF_8);*/
        /*FileUtils.copyURLToFile(
                new URL("http://www.bethelnewton.org/images/Passover_Guide_BOOKLET.pdf"),
                file);*/
            //File file = new File("http://www.bethelnewton.org/images/Passover_Guide_BOOKLET.pdf");
            String text = new PDFTextStripper().getText(doc);
            FileUtils.writeStringToFile(new File("test.txt"), text, StandardCharsets.UTF_8);

        } catch (IOException exception) {
            System.out.println(exception.getMessage());
        }

    }

    public static void ConvertPDFToHTML() {
        try {
            var file = new File("C:\\Users\\irisk\\OneDrive\\Documents\\university\\fourth year - first semester\\DSPS\\hw1\\src\\main\\java\\h1.pdf");
            PDDocument pdf = PDDocument.load(file);
            Writer output = new PrintWriter("pdf.html", StandardCharsets.UTF_8);
            new PDFDomTree().writeText(pdf, output);

            output.close();
        }catch (IOException exception)
        {
            System.out.println(exception.getMessage());
        }
    }

    public static void main(String[] argv){
        ConvertPDFToTextFile();
    }
}



