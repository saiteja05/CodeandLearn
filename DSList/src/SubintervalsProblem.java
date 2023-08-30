import java.util.Arrays;
import java.util.ArrayList;
import java.util.Comparator;
import java.lang.Integer;

public class SubintervalsProblem {

    public static void main(String[] args) {

        int[][] prob = {{1,3},{2,6},{4,8},{6,7},{15,55}};
        IntervalSol sol = new IntervalSol();
        for (Integer[][] i : sol.merge(prob))
        {
            System.out.println(i[0][0]+"-"+i[0][1]);

        }

    }
}

class IntervalSol {
    public ArrayList<Integer[][]> merge(int[][] intervals) {

        int n = intervals.length;
        Arrays.sort(intervals, new Comparator<int[]>() {
            @Override
            public int compare(int[] row1, int[] row2) {
                // Compare based on the first element of each row
                return Integer.compare(row1[0], row2[0]);
            }
        });

        ArrayList<Integer[][]> res = new ArrayList(n);
        Integer[][] t ={{intervals[0][0],intervals[0][1]}};
        res.add(t);

        for (int i=1;i<n;i++)
        {
            Integer[][] var = res.get(res.size()-1);
                    if(var[0][1] > intervals[i][0])
                    {
                        var[0][1]= Math.max(var[0][1],intervals[i][1]);
                        res.remove(res.size()-1);
                        res.add(var);
                    }
                    else
                    {
                        Integer [][] temp = {{intervals[i][0],intervals[i][1]}};
                        res.add(temp);
                    }
        }

        return  res;


    }
}

