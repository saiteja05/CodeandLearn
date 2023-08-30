import java.net.ServerSocket;
import java.util.Arrays;
public class NextPermutation {

    public static void  nextPermutation(int[] a) {

        int n = a.length;
        int i = -1;

        for (i=n-2;i>=0;--i)
            if(a[i]<a[i+1])
                break;
        if(i==-1)
            reverseArray(a,0,n-1);
        else
        {   System.out.println("found break: "+ i);
            int brp = i;
            int val = a[brp];
            for(i=n-1;i>brp;--i)
            {
                if(a[i]>val)
                {
                    swapArray(a,i,brp);
                    break;
                }
            }

            reverseArray(a,brp+1,n-1);
            printArray(a);

        }


    }

    static void  reverseArray(int[] a,int start,int end)
    {
        int left = start;
        int right = end;
        while(left<right)
        {
            swapArray(a,left,right);
            left++;
            right--;
        }
    }

    static void  swapArray(int[] a,int start,int end)
    {
        a[start]=a[start]+a[end];
        a[end]=a[start]-a[end];
        a[start]-=a[end];

    }


//        public static void nextPermutation(int[] a) {
//
//            int i=-1;
//            // finding common prefix,o to i-1 will be your common prefix
//            int n = a.length;
//            for (i = n-2 ; i>=0;i--)
//            {
//                if(a[i]<a[i+1])
//                    break;
//            }
//            System.out.println(i);
//            printArray(a);
//            if(i==-1)
//            {
//                System.out.println("i is -1");
//                Arrays.sort(a);
//               // printArray(a);
//            }
//            else
//            {
//                int brp=i;
//                int min_index = -1;
//                int min_value = 1000000;
//                //int breaker=a[brp];
//
//               // System.out.println(breaker);
//
//                for(i=n-1;i>=brp;i--)
//                {
//                    if( a[i] > a[brp]  && a[i]<min_value)
//                    {
//                     //System.out.println("her2");
//
//                        min_value=a[i];
//                        min_index=i;
//                    }
//
//                }
//          System.out.println(a[min_index]);
//
//                int temp = a[brp];
//                 a[brp]=a[min_index];
//                a[min_index]= temp;
//                printArray(a);
//                Arrays.sort(a,brp,n);
//
//
//
//
//            }
//
//
//
//        }

        public static void printArray(int[] a)
    {
        StringBuilder st = new StringBuilder(a.length);

        for (int i : a )
        {
            st.append(i+",");
        }


        System.out.println(st.toString());


    }

    public static void main(String[] args) {
        int[] nums={1,3,2};   //{2,1,5,4,3,0,0};

        nextPermutation(nums);


    }

}
