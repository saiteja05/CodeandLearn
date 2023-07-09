public class onewayedits {
    public static void main(String[] args) {
        String a ="taco";
        String b="stemhy";

        System.out.println(oneWayEdits(a,b));
    }

    //onewayedit delete 1 c, update 1 c, delete 1 c
    private static boolean oneWayEdits(String s1, String s2)
    {
        if (Math.abs(s1.length() - s2.length()) > 1)
        {
            return false;
        }

        String l = s1.length() < s2.length()?s2:s1;
        String s = s1.length()<s2.length()?s1:s2;

        int i = 0; int j =0;
        boolean ischanged= false;
        while (i<l.length() && j<s.length())
        {
            if(l.charAt(i)!=s.charAt(j))
            {
                if (ischanged)
                {
                    return false;
                }

                ischanged=true;
                if (l.length()==s.length())
                {
                    j++;
                }
            }
            else {
                j++;
            }



            i++;
        }


 return true;
    }

}
