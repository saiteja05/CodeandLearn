import java.util.*;


public class FindRepeatedSequences{

    public static void main(String[] args)
    {
        String s= "AAAAACCCCCAAAAACCCCCC";
        s.substring(0,8);
        HashSet<String> op = (HashSet) findRepeatedSequences( s, 8);

        for (String t : op)
        {
            System.out.println(t);
        }
    }
    public static Set<String> findRepeatedSequences(String s, int k) {

        HashSet<String> buffer = new HashSet<String>();
        HashSet<String> result = new HashSet<String>();
        int i=0;
        while((i+k)<s.length())
        {   //System.out.println(s.length()+"-"+(i+k)+"-i:"+i);
            String cur=s.substring(i,i+k);
            if(buffer.contains(cur))
            {
                result.add(cur);
            }
            buffer.add(cur);
            i++;

        }



        // Your code will replace this placeholder return statement
        return result;
    }
}