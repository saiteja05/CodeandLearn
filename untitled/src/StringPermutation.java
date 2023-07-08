import java.lang.reflect.Array;
import java.util.Arrays;
// check whether one string is permutation of another.
public class StringPermutation {

    public static void main(String[] args) {

        String a = new String("hello");
        String b = new String ("olleh");

      System.out.println(isPerumatation(a,b));



    }

    private static boolean isPerumatation(String a , String b)
    {
        Arrays.sort(a.toCharArray());
        Arrays.sort(b.toCharArray());
    return true;
    }


}
