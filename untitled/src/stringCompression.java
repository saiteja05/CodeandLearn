public class stringCompression {

    public static void main(String[] args) {

        String s= new String("aaabbbcccddeef");
       // a3b3c2d2e2f1;
        ;
        System.out.println(compress(s));

    }

    private static String compress(String s)
    {
        if (s.length()>2)
        {  int newlength =getCompressionLenggth(s);
            StringBuilder str = new StringBuilder((newlength));
            if ( newlength<s.length())
            {
                System.out.println("here");

                int count= 0 ;
                for (int i=0;i<s.length();i++)
                {      count++;
                    //System.out.println(s.charAt(i));
                    if (i+1>=s.length() || s.charAt(i)!=s.charAt(i+1))
                    {
                       str.append(s.charAt(i));
                       str.append(count);
                       count=0;
                    }
                }

            }
          return str.toString();
        }
return s;
    }

    private static int getCompressionLenggth(String s)
    {
        int clength =0 ;
        int count=0;
        for (int i=0;i<s.length();i++)
        {      count++;
           //System.out.println(s.charAt(i));
            if (i+1>=s.length() || s.charAt(i)!=s.charAt(i+1))
            {
                clength+= 1 + String.valueOf(count).length();
                count=0;
            }
        }
        //System.out.println(clength);
        return clength;
    }
}
