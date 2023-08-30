import java.util.ArrayList;
import java.util.List;

public class AllPermutationsBacktrack {

    public static void main(String[] args) {
        int[] nums ={1,2,3};
        new Permutation().permute(nums);
    }
}


class PermuteNoSpace1    {
    public List<List<Integer>> permute(int[] nums) {

        List<List<Integer>> ans = new ArrayList<>();
        permute(0,ans,nums);
        return ans;

    }

    public void permute(int index ,List<List<Integer>> ans,int[] nums)
    {

        if(index==nums.length)
        {
            ArrayList<Integer> al = new ArrayList<Integer>(nums.length);
            for(int j= 0 ; j<nums.length;j++)
                al.add(nums[j]);

            ans.add(al);
            return;
        }

        for (int i = index;i<nums.length;i++)
        {
            swapArray(nums,i,index);
            permute(index+1,ans,nums);
            swapArray(nums,i,index);
        }


    }

    public void swapArray(int[] a,int start,int end)
    {
        int t =a[start];
        a[start]=a[end];
        a[end]=t;
    }
}



class Permutation {

    public  void printArray(ArrayList<Integer> a)
    {
        StringBuilder st = new StringBuilder(a.size());

        for (int i : a )
        {
            st.append(i+",");
        }


        System.out.println(st.toString());


    }
    public List<List<Integer>> permute(int[] nums) {

        List<List<Integer>>  ans = new ArrayList<>();
        backtrack(new ArrayList<>(),ans,nums);
        return ans;


    }

    public void backtrack(ArrayList<Integer> cur, List<List<Integer>> ans, int[] nums )
    {


        if(cur.size()==nums.length)
        {
            System.out.println("result add");
            printArray(cur);
            ans.add(new ArrayList<>(cur));
            return;

        }

        for (int num:nums)
        {
            if(!cur.contains(num))
            {   System.out.println("adding: "+num);
                cur.add(num);
                System.out.println("Before");
                printArray(cur);
                backtrack(cur,ans,nums);
                System.out.println("after");
                printArray(cur);
                System.out.println("removing: "+cur.get(cur.size()-1));
                cur.remove(cur.size()-1);
                return;
            }
        }




    }
}


