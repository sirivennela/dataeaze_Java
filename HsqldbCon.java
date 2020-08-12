package hsqldb_csv;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.List;

public class HsqldbCon {
	
	public static void main(String[] args) {
		 Connection con = null;
	      Statement stmt = null;
	      
	      try {
	    	  //Registering the HSQLDB JDBC driver
		      Class.forName("org.hsqldb.jdbc.JDBCDriver");
		      //Creating the connection with HSQLDB
		      con = DriverManager.getConnection("jdbc:hsqldb:mem:Test", "SA", "");
		      if (con!= null){
		            System.out.println("Connection created successfully");
		            
		         }else{
		            System.out.println("Problem with creating connection");
		         }  
		      
		      stmt = con.createStatement();
		      stmt.execute("DROP TABLE IF EXISTS leedprojects_tbl;");
		      
		      String createCommand = "CREATE TABLE leedprojects_tbl(ID VARCHAR(20),\n"+
	            		"Isconfidential VARCHAR(100),\n" + 
		         		" ProjectName VARCHAR(100), \n" + 
		         		"Street VARCHAR(150),\n" + 
		         		" City VARCHAR(150),\n" + 
		         		" State VARCHAR(100),\n" + 
		         		" Zipcode VARCHAR(100),\n" + 
		         		" Country VARCHAR(100),\n" + 
		         		" LEEDSystemVersionDisplayName VARCHAR(100),\n" + 
		         		" PointsAchieved VARCHAR(100),\n" + 
		         		" CertLevel VARCHAR(100),\n" + 
		         		" CertDate VARCHAR(100),\n" + 
		         		" IsCertified VARCHAR(100), \n" + 
		         		"OwnerTypes VARCHAR(100),\n" + 
		         		" GrossSqFoot VARCHAR(100), \n" + 
		         		"TotalPropArea VARCHAR(100),\n" + 
		         		" ProjectTypes LONGVARCHAR, \n" + 
		         		"OwnerOrganization VARCHAR(100),\n" + 
		         		" RegistrationDate VARCHAR(100),\n" + 
		         		"PRIMARY KEY (id) );";
		      
		      stmt.executeUpdate(createCommand);
		      System.out.println("table created successfully");
		      
		      String path = "/home/socomo/Desktop/dataeaze_data/combinedata/";
		      final File folder = new File("/home/socomo/Desktop/dataeaze_data/combinedata");
		      List<String> files= listFilesForFolder(folder);
		      System.out.println(files.size());
		      for(int j=0;j<files.size();j++) {
		    	  System.out.println(files.get(j));
		    	 insertRecords(path+files.get(j),con);
		      }
		     // insertRecords(path,con);
		      
		      int totalRecords=getTotalRecords(con);
		      System.out.println("the records inserted are "+totalRecords);
		      
	      }
	      catch(Exception e) {
	    	  e.printStackTrace(System.out);
	      }
	     
	}
	
	private static void insertRecords(String file, Connection con) throws Exception {
   	 BufferedReader br = new BufferedReader(new FileReader(file));
   	 PreparedStatement ps = null;
   	 String line = null; 
   	 String Insertsql =
                "INSERT INTO leedprojects_tbl (ID,Isconfidential,ProjectName, Street,City,State,Zipcode,Country,"
                + "LEEDSystemVersionDisplayName,PointsAchieved, CertLevel,CertDate,IsCertified ,OwnerTypes,GrossSqFoot,"
                + "TotalPropArea,ProjectTypes,OwnerOrganization,RegistrationDate) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

   	 ps = con.prepareStatement(Insertsql);
        while ((line = br.readLine()) != null)   //returns a Boolean value  
        {  
       	 System.out.println(line);
       	 String data[]=line.split("\t");
       	 if((data.length)==19) {
       		 
       		String ID = data[0];
            String Isconfidential = data[1];
            String ProjectName = data[2];
            String Street = data[3];
            String City = data[4];	                
            String State = data[5];
            String Zipcode = data[6];
            String Country = data[7];
            String LEEDSystemVersionDisplayName = data[8];
            String PointsAchieved  = data[9];
            String CertLevel = data[10];
            String CertDate = data[11];
            String IsCertified = data[12];
            String OwnerTypes = data[13];
            String GrossSqFoot = data[14];
            String TotalPropArea = data[15];
            String ProjectTypes = data[16];
            String OwnerOrganization = data[17];
            String RegistrationDate = data[18];

            ps.setString(1, ID);
            ps.setString(2, Isconfidential);
            ps.setString(3, ProjectName);
            ps.setString(4, Street);
            ps.setString(5, City);
            ps.setString(6, State);
            ps.setString(7, Zipcode);
            ps.setString(8, Country);
            ps.setString(9, LEEDSystemVersionDisplayName);
            ps.setString(10, PointsAchieved);
            ps.setString(11, CertLevel);
            ps.setString(12, CertDate);
            ps.setString(13, IsCertified);
            ps.setString(14, OwnerTypes);
            ps.setString(15, GrossSqFoot);
            ps.setString(16, TotalPropArea);
            ps.setString(17, ProjectTypes);
            ps.setString(18, OwnerOrganization);
            ps.setString(19, RegistrationDate);
       	 
            
            ps.executeUpdate();
       	 }
       	 
        }
        
        con.commit();
        System.out.println("Insertion completed");
        
   }
	
	public static List<String> listFilesForFolder(final File folder) {
		//when i write the combined data from to local. It gives success file also
		
		List<String> filenames = new LinkedList<String>();
	    for (final File fileEntry : folder.listFiles()) {
	        if (fileEntry.isDirectory()) {
	            listFilesForFolder(fileEntry);
	        } else {
	        	//i am getting all part files and ignoring crc files in the success folder
	            if(fileEntry.getName().contains(".csv") & (fileEntry.getName().contains(".crc")==false))
	                filenames.add(fileEntry.getName());
	        }
	     }
	    return filenames;
	}
	
	private static int getTotalRecords(Connection con) throws Exception {
        try (Statement statement = con.createStatement();) {
            ResultSet result = statement.executeQuery("SELECT count(*) as total FROM leedprojects_tbl");
            if (result.next()) {
                return result.getInt("total");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return 0;
    }

}
//1000058274
