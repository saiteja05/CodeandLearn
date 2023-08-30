public class KadanesAlgo {
    public int maxSubArray(int[] nums) {

        int max = Integer.MIN_VALUE;
        int si = -1;
        int ei=-1;
        int local_max=0;

        for(int i = 0 ; i<nums.length;i++)
        {
            local_max+=nums[i];
            if(local_max>max)
            {
                max=local_max;
                if(si==-1)
                    si=i;
                ei=i;
            }
            if(local_max<0)
            {
                local_max=0;
                si=-1;
            }
        }



//if empty set case needs to be handled, you need to return 0 is sum<0
        return max;

    }
}
