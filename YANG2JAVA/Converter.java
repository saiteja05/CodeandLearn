package YANG2JAVA;
import java.io.IOException;
import java.util.ArrayList;

public class Converter {
	static java.util.Hashtable<String,String> lookup = new java.util.Hashtable<String,String>();

 

	 static boolean isLL,istypedef ;
	   static boolean isl;
	 
	   
	   public static void checkName(ArrayList lexems,String name,String token) throws ParseException
	   {
		   if(lexems.contains(name))
			{
				throw new ParseException("The "+ token+" : "+name+" is already declared elsewhere , Please choose a different identifier.");
			}
		  else
			{
				lexems.add(name);
			} 
	   }
	   
	   
	static public void convert(String path,String name) throws IOException, ParseException
	{	
		ArrayList<String> lexems = new ArrayList<String>();
		ArrayList<String> lexem = new ArrayList<String>();
		//boolean mopen=false;
	
		String end = ";";
		String n="";
		java.util.Stack<String> par = new java.util.Stack<String>();
		java.util.Stack<String> keys = new java.util.Stack<String>();
		//java.util.Stack<String> temp = new java.util.Stack<String>();
		
			populate();
			java.io.BufferedWriter bw = new java.io.BufferedWriter(new java.io.FileWriter(path+"/"+name+".java"));
		
			java.io.BufferedReader br = new java.io.BufferedReader(new java.io.FileReader(path+"/"+name+".yang"));

			String sCurrentLine;

			
			bw.write("import java.lang.*;\n");
			bw.write("import java.util.*;\n");
			int type = 0;
			while ((sCurrentLine = br.readLine()) != null ) {
				
					
						String [] tokens = sCurrentLine.split(" |\\t|\b");
						
						for(int i =0;i<tokens.length;i++)
						{
						//	
							
							if(!(tokens[i].equals("")))
							{
								//System.out.println("Token:"+tokens[i]);
								//System.out.println("OUT :"+isl);
								if(lookup.containsKey(tokens[i]))
								{
									
									
									if(istypedef)
									{
										
										if(tokens[i].equals("type"))
										{		
											
											if(lookup.containsKey(tokens[i+1]))
											{
												bw.write(lookup.get(tokens[i+1])+" temp"+(type++)+";");
											}
											
											istypedef=false;
									}
								}
								 
							
									
									
									
									if(isLL)
									{
										if(tokens[i].equals("type"))
										{
											if(lookup.containsKey(tokens[i+1]))
											{		
											bw.write(lookup.get(tokens[i+1])
													+">");
											bw.write(" ");
											bw.write(n);
											
											bw.write("new List<>()"+end);
											bw.write("\n");
											}
											n="";
											
											isLL=false;
										}
									}
										 if(isl)
										{
											
											if(tokens[i].equals("type"))
											{ 
												if(lookup.containsKey(tokens[i+1]))
												{
												bw.write("\n");
													
												bw.write(lookup.get(tokens[i+1]));
												bw.write(" ");
												bw.write(n);
												bw.write(end);
												if(i+4==tokens.length)
												{	
													bw.write("\n");}
												}
												
												isl=false;
												
												
												
										}
												
												
										}
									
									if(tokens[i].equals("module"))
											{ checkName(lexems,tokens[i+1],tokens[i]);
												bw.write("\t//"+tokens[i]);
												keys.push("module");
										
												bw.write("\n");
												
												bw.write(lookup.get("module"));
												bw.write(" ");
												bw.write(tokens[i+1]);
												bw.write(" ");
											bw.write("\n{");
												bw.write("\n");
												
												//par.push("}");
											//	mopen=true;
											}
									else if(tokens[i].equals("typedef"))
									{	checkName(lexems,tokens[i+1],tokens[i]);
										bw.write("\t//"+tokens[i]);
										keys.push(tokens[i]);
										bw.write("\n");
										bw.write("class");
										bw.write(" ");
										bw.write(tokens[i+1]);
										lookup.put(tokens[i+1],tokens[i+1]);
										
										bw.write("\t{");
										bw.write("\n");
										//par.push("}");
										istypedef=true;
										
									}
									else if(tokens[i].equals("{"))
									{
										
										par.push("{");
										
									
									}
									else if(tokens[i].equals("}"))
									{
										
										par.push("}");
										
										if(!keys.empty())	
										{	
											if(keys.peek().equals("typedef"))
										
											{	par.pop();
												par.pop();
												bw.write("\n}");
												keys.pop();
												lexem = new ArrayList<String>();
											}
											else if(keys.peek().equals("leaf")||keys.peek().equals("leaf-list"))
											{
												par.pop();
												par.pop();
												keys.pop();
											}
											else if(keys.peek().equals("list"))
											{
												keys.pop();
												par.pop();
												par.pop();
												bw.write("\n}");
												
											}
											else if(keys.peek().equals("module"))
											{
												keys.pop();
												par.pop();
												par.pop();
												bw.write("\n}");
												lexem = new ArrayList<String>();
											}
											else if(keys.peek().equals("container")||keys.peek().equals("notification")||keys.peek().equals("grouping"))
											{
												keys.pop();
												par.pop();
												par.pop();
												bw.write("\n}");
												lexem = new ArrayList<String>();
											}
											else if(keys.peek().equals("import"))
											{
												keys.pop();
												par.pop();
												par.pop();
												
											}
										}
									}
									else if(tokens[i].equals("container"))
									{checkName(lexems,tokens[i+1],tokens[i]);
										bw.write("\t//"+tokens[i]);
										bw.write("\n");
										keys.push(tokens[i]);
										bw.write(lookup.get(tokens[i]));
										bw.write(" ");
										bw.write(tokens[i+1]);
										bw.write("\t{");
										bw.write("\n");
										//par.push("}");
									}
									else if(tokens[i].equals("leaf-list"))
									{
										keys.push(tokens[i]);
										isLL = true;
										bw.write("List<");
										n=tokens[i+1];
										if(lexem.contains(n))
										{
											throw new ParseException("The Leaf-List : "+n+" already declared , Please choose a different identifier.");
										}
										else
										{
											lexem.add(n);
										}
										//  System.out.println(n);
										
									}
									else if(tokens[i].compareTo("leaf")==0)		
									{ isl=true;
									  n = tokens[i+1];
									  keys.push("leaf");
									  if(lexem.contains(n))
										{
											throw new ParseException("The Leaf  : "+n+" already declared , Please choose a different identifier.");
										}
									  else
										{
											lexem.add(n);
										}
									//  System.out.println("In: "+isl);
								
								
									}
									else if(tokens[i].equals("list"))
									{	
										checkName(lexems,tokens[i+1],tokens[i]);
										bw.write("\t//"+tokens[i]);
										keys.push("list");
										bw.write("\n");
										bw.write(" ");
										bw.write(lookup.get(tokens[i]));
										bw.write(" ");
										bw.write(tokens[i+1]);
										bw.write("\t{");
										bw.write("\n");
										//par.push("}");
									}
									else if(tokens[i].equals("grouping")||tokens[i].equals("notification"))
									{ 
										checkName(lexems,tokens[i+1],tokens[i]);
										keys.push(tokens[i]);
										bw.write("\t//"+tokens[i]);
										bw.write("\n");
										bw.write(" ");
										bw.write(lookup.get(tokens[i]));
										bw.write(" ");
										bw.write(tokens[i+1]);
										bw.write("\t{");
										bw.write("\n");
										//par.push("}");
										
									}
									else if(tokens[i].equals("default"))
									{
										bw.write(n+" = "+tokens[i+1]+end);
										bw.write("\n");
									}
								/*	else if(tokens[i].equals("mandatory"))
									{
										bw.write(end+"\n");
									}*/
									else if(tokens[i].equals("import"))
									{
										keys.push("import");
									}
							}
						}
					
					
			}
						
			}
		

	
	br.close();
	bw.close();
	}

	private static void populate() {
		
		// TODO Auto-generated method stub
		lookup.put("module","class");
		lookup.put("container","interface");
		lookup.put("grouping","interface");
		lookup.put("leaf","leaf");
		lookup.put("type","type");
		lookup.put("list","class");
		lookup.put("leaf-list","List");
		lookup.put("notification","interface");
		lookup.put("typedef","class");
		lookup.put("boolean","Boolean");
		lookup.put("bit","Boolean");
		lookup.put("empty","Boolean");
		lookup.put("int8","Byte");
		lookup.put("int16","Short");
		lookup.put("int32","Integer");
		lookup.put("int64","Long");
		lookup.put("string","String");
		lookup.put("yang:date-and-time","Date");
		lookup.put("default","default");
		lookup.put("decimal64","Double");
		lookup.put("uint8","Byte");
		lookup.put("uint16","Short");
		lookup.put("uint32","Integer");
		lookup.put("uint64","BigInteger");
		lookup.put("binary","Byte[]");

		lookup.put("identity","abstract class");
		lookup.put("{","{");
		lookup.put("}","}");
	    lookup.put("mandatory","");
	    lookup.put("import","");
	}
	
	
}
