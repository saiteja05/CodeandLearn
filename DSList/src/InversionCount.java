public class InversionCount {
    public static void main(String[] args) {
        long[] a = {52244275, 123047899, 493394237, 922363607, 378906890, 188674257, 222477309, 902683641, 860884025, 339100162};
        //{4,3,2,1,5,3,6,4,2};
        // Write your code here.
        MergeSortSolution sol = new MergeSortSolution();
        System.out.println("inversio count :"+ sol.mergeSort(a, 0, a.length - 1));
        sol.printArray(a);
    }
}
  class MergeSortSolution
        {
            public  void printArray(long[] a)
            {
                StringBuilder st = new StringBuilder(a.length);
                for (long i : a )
                {
                    st.append(i+",");
                }

                System.out.println(st.toString());


            }
            public long mergeSort(long [] a , int low , int high)
            {   long count=0;
                if(low>=high)
                {
                    return 0;
                }

                int mid=(low+high)/2;
                //System.out.println("mid is "+mid+" low is "+low+" high is "+high);
                count+= mergeSort(a,low,mid);
                count+=mergeSort(a,mid+1,high);
                count+=merge(a,low,mid+1,high);


                return count;
            }

            public long merge(long [] a , int low, int mid,int high)
            {
                //System.out.println("merging mid is "+mid+" low is "+low+" high is "+high);
                long[] temp = new long[(high-low)+1];
                int index=0;
                int left= low;
                int right=mid;
                long count = 0;

                //inital check and swaps.
                while (left < mid && right<=high )
                {
                    if (a[left]>a[right])
                    {// System.out.println(a[s]+">"+a[m]+"adding lowest at "+index);
                        temp[index++]= a[right++];
                        count+=mid-left;
                    }
                    else if(a[left]<=a[right])
                    {
                        // System.out.println(a[s]+"<="+a[m]+"adding lowest at "+index);
                        temp[index++]=a[left++];
                    }
                }


                //merging the remaining part
                // printArray(temp);
                if(left < mid)
                {
                    for (int i = left; i<mid;i++)
                    { //  System.out.println(" s< mid adding "+a[i]);
                        temp[index++]=a[i];
                    }

                }
                if(right<=high)
                {
                    for (int i = right; i<=high;i++)
                    {  //System.out.println(" m <=high adding "+a[i]);
                        temp[index++]=a[i];
                    }

                }


                index=0;

                for (int i = low;i<=high;i++)
                {
                    a[i]=temp[index++];
                }

                // printArray(temp);
                //printArray(a);

                return count;
            }

        }