public class palindromePermutations {
    //tacotac
    //cbc
    public static void main(String[] args) {

        String s = "caz";

        System.out.println(isPalindrome(s) );
    }

    private static boolean isPalindrome(String s)
    {
        int bitvector = 0;
        bitvector=buildBitVector(s,bitvector);
        System.out.println(bitvector);
        return bitvector==0||checkBitVector(bitvector);

    }

    private static int buildBitVector(String s, int bitvector)
    {
        for (char c : s.toCharArray())
        {
            int x = getCharacterNumeric(c);
           // System.out.println(x);
            if(x>0)
            {


                bitvector=toggle(bitvector,x);
                //System.out.println(bitvector);

            }
        }
        return  bitvector;
    }

    private static int toggle(int bitvector,int index)
    {
        int mask = 1 << index;

        if ((bitvector & mask)== 0){
             bitvector |= mask;}
        else {
            bitvector = bitvector & ~mask;
        }

        return bitvector;
    }

    private static int getCharacterNumeric(char c)
    { //assumingthatvaluesarebetweena&z
        int a = Character.getNumericValue('a');
        int z = Character.getNumericValue('z');
        int val=Character.getNumericValue(c);
        if (val>=a && val <= z)
        {
            return val-a;
        }
        else
        {
            return -1;
        }

    }

    private static boolean checkBitVector(int bitvector)
    {
       return ((bitvector) & (bitvector - 1) )==0;
    }
}
