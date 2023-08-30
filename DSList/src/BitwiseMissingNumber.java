public class BitwiseMissingNumber {

    public static void main(String[] args) {

        int[] a  = {1,2,3,4,5,6,1,7,9,8};

        BitwiseSolution b = new BitwiseSolution();

       int[] res =b.repeatedNumber(a);

       System.out.println("Duplicate number is : "+res[0]+" missing number is "+res[1]);
    }

}
/*
other solution
simply do
for 1-n
(sum of all given elements ) - (sum of n elements)
(sum of all given elemtns ) - (sum of n squares)

you will have x-y and x2-y2, solve for x  and y.
*/

 class BitwiseSolution {
    // DO NOT MODIFY THE ARGUMENTS WITH "final" PREFIX. IT IS READ ONLY
    public int[] repeatedNumber(final int[] a) {

        int n = a.length;
        int xr=0;

        for (int i=0 ; i<n;i++)
        {
            xr^=a[i];
            xr^=(i+1);
        }

        int bitnumber = 0;

        while (true)
        {
            if((xr & (1 << bitnumber)) !=0)
            {
                break;
            }

            ++bitnumber;

        }
        int zero = 0;
        int one = 0 ;

        for(int i =0 ; i<n;i++)
        {
            if((a[i]&(1<<bitnumber)) !=0)
            {
                one ^=a[i];
            }
            else
            {
                zero^=a[i];
            }

            if(((i+1)&(1<<bitnumber))!=0)
            {
                one^=(i+1);
            }
            else
            {
                zero^=(i+1);
            }
        }

        int count =0;

        for(int i=0;i<n;i++)
        {
            if(a[i]==zero)
            {
                ++count;
            }
        }
        int [] res = new int[2];
        if(count>1)
        {
            res[0]=zero;
            res[1]=one;
        }

        else
        {
            res[0]=one;
            res[1]=zero;

        }


        return res;


    }
}
