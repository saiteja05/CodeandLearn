import java.util.Hashtable;
import java.util.Arrays;

// Press ⇧ twice to open the Search Everywhere dialog and type `show whitespaces`,
// then press Enter. You can now see whitespace characters in your code.
 Public class UniqueString {
    public static void main(String[] args) {
        // Press ⌥⏎ with your caret at the highlighted text to see how
        // IntelliJ IDEA suggests fixing it.
        int[] a;
        a = new int[]{};

        // Press ⌃R or click the green arrow button in the gutter to run the code.

        System.out.println(isUnique2(a));

    }

    private static boolean isUnique2(int[] a)
    {
        if(a.length<=1){
            return true;

        }
        if (a.length==2)
        {
            return !(a[0]==a[1]);
        }
        Arrays.parallelSort(a);
       // System.out.println(Arrays.toString(a));
        for (int i=1; i<a.length;i++)
        {
            if(a[i]==a[i-1])
            {
                return false;
            }
        }
        return true;

    }

    private static boolean isUnique(int[] a)
    {
        Hashtable<Integer,Integer> lookup = new Hashtable<Integer,Integer>();

        for (int i = 0; i < a.length; i++) {

            if(lookup.get(a[i])!=null)
            {
                return false;

            }
            lookup.put(a[i],1);
            // Press ⌃D to start debugging your code. We have set one breakpoint
            // for you, but you can always add more by pressing ⌘F8.


        }
        return true;
    }
}

// implement a arraylist of linkedlists. (linkedlists because of collisions)