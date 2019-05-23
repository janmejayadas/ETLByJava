package com.sql.etl.test;

	import java.io.BufferedReader;
	import java.io.FileInputStream;
	import java.io.FileWriter;
	import java.io.IOException;
	import java.io.InputStreamReader;
	import java.io.StringReader;
	import java.util.ArrayList;
	import java.util.List;

	import net.sf.jsqlparser.JSQLParserException;
	import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
	import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.alter.AlterExpression.ColumnDataType;
import net.sf.jsqlparser.statement.create.table.ColDataType;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
	import net.sf.jsqlparser.statement.create.table.CreateTable;
	import net.sf.jsqlparser.statement.create.table.Index;
	import net.sf.jsqlparser.statement.drop.Drop;
	import net.sf.jsqlparser.statement.insert.Insert;

	public class ApiPoc {

			public static String[] readSql(String schema) throws IOException {
			    BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(schema)));
			    String mysql = "";
			    String line;
			    
			    while ((line = br.readLine()) != null) {
			    	mysql = mysql + line;
			    	if(null!=mysql) {
				    	   mysql=mysql.replace("_status` ("," ( \r"+"`devId` char(12) NOT NULL  DEFAULT NULL,");
				       }
			    }
			    br.close();
			    mysql = mysql.replaceAll("`", "");
			    System.out.println(mysql);
			    return mysql.split(";");
			}

			public static List<String> getColumnNames(String tableName, String schemaFile) throws JSQLParserException, IOException {
			    CCJSqlParserManager pm = new CCJSqlParserManager();
			    List<String> columnNames = new ArrayList<String>();
			    List<String> columSpeString=new ArrayList<String>();
			      columSpeString.add("NOT");
			      columSpeString.add("NULL");
			      columSpeString.add("DEFAULT");
			      columSpeString.add("NULL");//[NOT, NULL, DEFAULT, NULL]
			      
			    String[] sqlStatements = readSql(schemaFile);
			    for (String sqlStatement : sqlStatements) {
			        Statement statement = pm.parse(new StringReader(sqlStatement));
			       //`row_id` int(11) NOT NULL DEFAULT '1',
			        
			        
			        /* if (statement instanceof CreateTable) {
			        	CreateTable create = (CreateTable) statement;
		
			 
			        	
			        	List<ColumnDefinition> columns = create.getColumnDefinitions();
			            	 List<String> ls=null;
			                for (ColumnDefinition def : columns) {
			                   ls=def.getColumnSpecStrings();
			                  // System.out.println(ls);
			                System.out.println("Names="+def.getColumnName());
			                System.out.println("types="+def.getColDataType());
			                   columnNames.add(def.getColumnName()+" "+def.getColumnSpecStrings());
			                     // columnNames.add(def.getColDataType().getDataType());
			                }
			                
			                //break;
			           }
			    }*/
			    }   
			    return columnNames;
			}
	//
			    
			    public static List<String> getType(String tableName, String schemaFile) throws JSQLParserException, IOException {

				    CCJSqlParserManager pm = new CCJSqlParserManager();
				    List<String> dataTypes = new ArrayList<String>();
		              
				    String[] sqlStatements = readSql(schemaFile);
		            for (String sqlStatement : sqlStatements) {
				    	
				        Statement statement = pm.parse(new StringReader(sqlStatement));
		                      
				        if (statement instanceof CreateTable) {
				        
				        	CreateTable create = (CreateTable) statement;
				            String name = create.getTable().getName();
		                         
				          //  if (name.equalsIgnoreCase(tableName)) {
				            	 List<ColumnDefinition> columns = create.getColumnDefinitions();
				   //ColumnDataType colDataType=new ColumnDataType("devId", withType, colDataType, columnSpecs)
				   
				    //       	 List<Index> listOfIndex=create.getIndexes();
				   //         	 List<String> ls=null;
				                for (ColumnDefinition def : columns) {
				       //         	 ls=def.getColumnSpecStrings();
				   //             	 System.out.println(ls);
				                	
				                	dataTypes.add(def.getColDataType().getDataType());
				                      
				                }
				                break;
				           // }
				        }
				    }

				    
			    
			    return dataTypes;
			}
	//
			    
			    
			    public static List<String> getIndexs(String tableName, String schemaFile) throws JSQLParserException, IOException {

				    CCJSqlParserManager pm = new CCJSqlParserManager();
				    List<String> indexs =new ArrayList<String>();
		              
				    String[] sqlStatements = readSql(schemaFile);
		            for (String sqlStatement : sqlStatements) {
				    	
				        Statement statement = pm.parse(new StringReader(sqlStatement));
		                      
				        if (statement instanceof CreateTable) {
				        
				        	CreateTable create = (CreateTable) statement;
				            String name = create.getTable().getName();
		                         
				            if (name.equalsIgnoreCase(tableName)) {
				            	 //List<ColumnDefinition> columns = create.getColumnDefinitions();
				            	 
				            	 List<Index> listOfIndex=create.getIndexes();
				   //         	 List<String> ls=null;
				                for (Index  index: listOfIndex) {
				                	
				                	indexs.add(index.getName());
				                      
				                }
				                break;
				            }
				        }
				    }

				    
			    
			    return indexs;
			}
			    
	//
			    public static List<String> changeTableName(String tableName, String schemaFile,String newTblName,String suffix) throws JSQLParserException, IOException 
			    {
				    CCJSqlParserManager pm = new CCJSqlParserManager();
				    List<String> listOfStr = new ArrayList<String>();
		              String newTableFormat=null;
				    String[] sqlStatements = readSql(schemaFile);
				  //  System.out.println("-------------------------------");
				    for (String sqlStatement : sqlStatements) {
				  	        Statement statement = pm.parse(new StringReader(sqlStatement));
		                      
				        if (statement instanceof CreateTable) {
				        
				        	CreateTable create = (CreateTable) statement;
				           // String name = create.getTable().getName();
				            
				            create.setTable(new Table(newTblName));
				            //List<ColumnDefinition> col=create.getColumnDefinitions();
				            //System.out.println(col);
				           // System.out.println(create.toString()+"; \r");
				            listOfStr.add(create.toString()+"; \r");
				        }else if(statement instanceof Drop) {
				        	
				        	Drop droptable=(Drop)statement;
				        	
				        	droptable.setName(new Table(newTblName+"; \r"));
				        	
				        	//System.out.println(droptable.toString());
				        	listOfStr.add(droptable.toString());
				        }
				       if (statement instanceof Insert) {
					        
				        	Insert insert = (Insert) statement;
				            String name = insert.getTable().getName();
				            if(name.endsWith(suffix)) {
				            
				            insert.setTable(new Table(newTblName));
				            }
				            listOfStr.add(insert.toString()+"; \r");
				        }
				    }
				    return listOfStr;		        
				    }
				    
		
//				}

			    
			    public static List<String> changeInsertStmt(String tableName, String schemaFile) throws JSQLParserException, IOException {

				    CCJSqlParserManager pm = new CCJSqlParserManager();
				    List<String> listOfStr = new ArrayList<String>();
		              String newTableFormat=null;
				    String[] sqlStatements = readSql(schemaFile);
				  //  System.out.println("-------------------------------");
				    for (String sqlStatement : sqlStatements) {
				  	        Statement statement = pm.parse(new StringReader(sqlStatement));
		                      
				  	        if (statement instanceof Insert) {
				        
				        	Insert insert = (Insert) statement;
				            String name = insert.getTable().getName();
				            if(name.endsWith("_status")) {
				            
				            insert.setTable(new Table("bb232_status"));
				            }
				            listOfStr.add(insert.toString()+"; \r");
				        }
				    }
				    
				    return listOfStr;
				}
			    

			/*    
			public static void main(String[] args) throws Exception {

			    String schemaFile = "C:\\Users\\jandas\\eclipse-workspace\\ETL_zql\\src\\1065a302_events.sql";
		    String tableName = "_1065a3020000_events";
		  //  String dataFile = "C:\\Users\\jandas\\eclipse-workspace\\ETL_zql\\src\\1065a302_status_data.sql";
		    
			  //  List<String> columnNames = BUDataColumnsFinder.getColumnNames(tableName, schemaFile);
			    //List<String> dataTypes = BUDataColumnsFinder.getType(tableName, schemaFile);
			                 
		    String newTablName="bb232_events";
		    String suffix="_events";
		System.out.println("start...");
		
		List<String> listOfStr=ApiPoc.changeTableName(tableName, schemaFile,newTablName,suffix);
//		List<String> listOfInsrt=BUDataColumnsFinder.changeInsertStmt(tableName, dataFile);
		
		FileWriter fw=new FileWriter("C:\\Users\\jandas\\eclipse-workspace\\ETL_zql\\src\\output.sql");
		boolean drpFlag=false;
		boolean crtFlag=false;
		boolean istFlag=false;
		int dcount=0;
		int ccount=0;
		String drop="";
		String creat="";
		boolean mismatch=false;
		for(int i=0;i<listOfStr.size();i++) {
				for(int j=i+1;j<listOfStr.size();j++) {
					if((listOfStr.get(i).startsWith("DROP")) && (listOfStr.get(j).startsWith("DROP"))) {
						//System.out.println(true);
						
					if(listOfStr.get(i).equals(listOfStr.get(j))) {
						drpFlag=true;
						//System.out.println(listOfStr.get(i)+"--"+listOfStr.get(j));
					//	fw.write(listOfStr.get(i));
						drop=listOfStr.get(i);
						dcount++;
					}
					}
					else if((listOfStr.get(i).startsWith("CREATE"))  && (listOfStr.get(j).startsWith("CREATE")))
					{
						//System.out.println(listOfStr.get(i)+"--"+listOfStr.get(j));	
						
						if(listOfStr.get(i).equals(listOfStr.get(j))) {
							crtFlag=true;
							//System.out.println(listOfStr.get(i)+"--"+listOfStr.get(j));
							//fw.write(listOfStr.get(i));
							creat=listOfStr.get(i);
							ccount++;
						}else {
							System.out.println("TableMismatch !!!!!");
							System.out.println(listOfStr.get(i)+"--"+listOfStr.get(j));
							mismatch=true;
							break;
						}
					}
				if(mismatch) {
					break;
				}
			}
				if(mismatch) {
					break;
				}
		}
		
		
		if(drpFlag && crtFlag && mismatch!=true) {
			fw.write(drop);
			fw.write(creat);
		}
		for(int i=0;i<listOfStr.size();i++) {
		 if((listOfStr.get(i).startsWith("INSERT INTO"))) {
			fw.write(listOfStr.get(i)); 
		}
		}
		for(int i=0;i<listOfInsrt.size();i++) {
			if(drpFlag && crtFlag && mismatch!=true) {
			fw.write(listOfInsrt.get(i));
			}
		}
		fw.flush();
		fw.close();
		
			}*/
	}
	
	

