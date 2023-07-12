import java.util.HashSet;
import java.util.StringTokenizer;

// input= '    hello world' , ouput= 'world hello'
public class StringWordFlip {
    public static void main(String[] args) {

        String s = new String(" Hello          world   ");
        System.out.println("input : "+s);
        //easiest way
//        StringTokenizer st = new StringTokenizer(s," ");
//        StringBuilder b = new StringBuilder(s.length());
//        while(st.hasMoreTokens())
//        {
//           b.insert(0," "+st.nextToken());
//        }
//        System.out.println(b.toString());

//        reverseTokenize(s);
//        HashSet<String> ts = new HashSet<String>();
//
//        ts.add("S");
//        ts.add("S");
//        ts.add("N");
//    for( String t : ts)
//        {
//            System.out.println(t);
//        }
    }


    private static void reverseTokenize(String s) {


        int start = 0;
        int end = s.length() - 1;
        //reverse the entire string and eliminate leading spaces.

        StringBuilder b = new StringBuilder(s);
        while (b.charAt(start) == ' ') ++start;
        while (b.charAt(end) == ' ') --end;

        b.delete(end + 1, b.length());
        b.delete(0, start);
        //System.out.println(b.toString());
        singleSpace(b);
        reverseString2(b, 0, b.length() - 1);
        //System.out.println(b.length());
        //System.out.println(b.toString());
        start = 0;
        end = 0;

        while (start < b.length()) {

            while (end < b.length() && b.charAt(end) != ' ') ++end;

            reverseString2(b, start, end - 1);

            start = ++end ;


        }

        System.out.println(b.toString());


    }

    //inclusiveend
    private static void reverseString2(StringBuilder s, int start, int end) {
        while (start < end) {
            char temp = s.charAt(start);
            s.setCharAt(start++, s.charAt(end));
            s.setCharAt(end--, temp);

        }
    }

    public static void singleSpace(StringBuilder b)
    {
        int slow = 0;
        int fast= 1;

        while (fast<b.length())
        {
            if(b.charAt(slow)==' ' && b.charAt(fast)==' ')
            {
                b.deleteCharAt(fast);
            }
            else
            {
                slow++;
                fast++;
            }
        }


    }

//    private  static void reverseString2(StringBuilder s, int start,int end)
//    {
//        boolean firstspace = false;
//
//        while (start<end)
//        {
//
//            if(s.charAt(start)==' ' && !firstspace)
//            {
//                firstspace= true;
//                while(s.charAt(start+1)==' ' && firstspace)
//                {
//                    s.deleteCharAt(start);
//                    start++;
//                }
//
//            }
//
//            char temp = s.charAt(start);
//            s.setCharAt(start, s.charAt(end));
//            s.setCharAt(end, temp);
//
//
//            start++;
//            end--;
//
//
//        }
//  }


    }




