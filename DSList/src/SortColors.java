public class SortColors {

    public static void main(String[] args)
    {
        Solution sol = new Solution();
        int[] arr = {2};
        sol.sortColors(arr);
        sol.printArray(arr);
    }
}

class Solution {

    public  void printArray(int[] a)
    {
        StringBuilder st = new StringBuilder(a.length);

        for (int i : a )
        {
            st.append(i+",");
        }


        System.out.println(st.toString());


    }

    public void sortColors(int[] a) {

        int left =0;
        int mid=0;
        int right = a.length-1;

        while(mid<=right)
        {
            if(a[mid]==0)
            {
                swapArray(a,mid,left);
                mid++;
                left++;
                System.out.println("0 found");


            }

            else if(a[mid]==2)
            {
                swapArray(a,mid,right);
                --right;
                System.out.println("2 found");


            }
            else if(a[mid]==1)
            {
                mid++;
                System.out.println("1 found");

            }

            System.out.println(a[mid]);


        }



    }

    void swapArray(int a[],int start,int end)
    {
        if(start != end) {
            a[start] = a[start] + a[end];


            a[end] = a[start] - a[end];

            a[start] -= a[end];
        }


    }
}