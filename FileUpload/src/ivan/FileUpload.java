package ivan;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.commons.fileupload.FileItem;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.fileupload.servlet.ServletFileUpload;
import java.sql.*;
 
@WebServlet("/FileUpload")
public class FileUpload extends HttpServlet {
  private static final long serialVersionUID = 1L;
    
  // parameter setting
  private static final int MEMORY_THRESHOLD  = 1024 * 1024 * 3; // 3MB
  private static final int MAX_FILE_SIZE   = 1024 * 1024 * 40; // 40MB
  private static final int MAX_REQUEST_SIZE  = 1024 * 1024 * 50; // 50MB
   
  public FileUpload() {
    super();
  }
  
  static void insert(String csvFilePath) {
      String jdbcURL = "jdbc:mysql://changeme:3306/demo?useSSL=false";
      String username = "changeme";
      String password = "changeme";

      int batchSize = 20;

      Connection connection = null;

      try {
    	  try {
			Class.forName("com.mysql.cj.jdbc.Driver");
    	  } catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
    	  } 
          connection = DriverManager.getConnection(jdbcURL, username, password);
          connection.setAutoCommit(false);

          String sql = "INSERT INTO product_list (id, product, quantity) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE product = VALUES(product), quantity = VALUES(quantity)";
          PreparedStatement statement = connection.prepareStatement(sql);

          BufferedReader lineReader = new BufferedReader(new FileReader(csvFilePath));
          String lineText = null;

          int count = 0;

          lineReader.readLine(); // skip header line

          while ((lineText = lineReader.readLine()) != null) {
              String[] data = lineText.split(",");
              String id = data[0];
              String product = data[1];
              String quantity = data[2];

              statement.setString(1, id);
              statement.setString(2, product);
              statement.setString(3, quantity);

              statement.addBatch();

              if (count % batchSize == 0) {
                  statement.executeBatch();
              }
          }

          lineReader.close();

          // execute the remaining queries
          statement.executeBatch();

          connection.commit();
          connection.close();

      } catch (IOException ex) {
          System.err.println(ex);
      } catch (SQLException ex) {
          ex.printStackTrace();

          try {
              connection.rollback();
          } catch (SQLException e) {
              e.printStackTrace();
          }
      }
  }
   
  protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
     
    request.setCharacterEncoding("UTF-8");
    response.setCharacterEncoding("UTF-8");
    response.setContentType("text/html;charset=UTF-8");
    PrintWriter out = response.getWriter();
     
      if (!ServletFileUpload.isMultipartContent(request)) {
      out.println("Error: Not included enctype=multipart/form-data");
      out.flush();
      return;
    }
     
    // upload factory
    DiskFileItemFactory factory = new DiskFileItemFactory();
    // set memory threshold for size
    factory.setSizeThreshold(MEMORY_THRESHOLD);
    // set temp dir
    factory.setRepository(new File(System.getProperty("java.io.tmpdir")));
  
    ServletFileUpload upload = new ServletFileUpload(factory);
      
    // set max file size
    upload.setFileSizeMax(MAX_FILE_SIZE);
      
    // set max request siez
    upload.setSizeMax(MAX_REQUEST_SIZE);
     
    // handle other text
    upload.setHeaderEncoding("UTF-8");
     
     
    String uploadPath = request.getServletContext().getRealPath("/files");
    File uploadDir = new File(uploadPath);
    if (!uploadDir.exists()) {
      uploadDir.mkdir();
    }
     
    try {
      List<FileItem> formItems = upload.parseRequest(request);
      if (formItems != null && formItems.size() > 0) {
        // for each file items
        for (FileItem item : formItems) {
          if (!item.isFormField()) {
            String fileName = new File(item.getName()).getName();
            String filePath = "D:\\upload_files" + File.separator + fileName;
            File storeFile = new File(filePath);
            // print output path in console
            System.out.println(filePath);
            // save the file into D drive
            if(storeFile.exists()) {
              out.println("Upload failed, the file exists already£¡");
            }else {
              item.write(storeFile);
              out.println("Upload successfully!");
              insert(filePath);
            } 
          }
        }
      }
    } catch (Exception ex) {
      response.getWriter().println("Upload successfully!");
    }
  }
 
   
  protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
    doGet(request, response);
  }
}