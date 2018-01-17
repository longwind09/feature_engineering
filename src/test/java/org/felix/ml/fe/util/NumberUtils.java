package org.felix.ml.fe.util;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 *  1/6/2018.
 * function:
 * last updated:1/6/2018
 */
@RunWith(JUnit4.class)
public class NumberUtils {
    @Test
    public void test() {
        int[] arr = new int[]{0, 1, 2, 3, 4, 10};
        int k = 2;
        int ret = get_nth(arr, 1000);
        System.out.println(ret);
    }

    int partition(int[] arr, int low, int high) {
        int x = arr[low];
        while (low < high) {
            while (low < high && arr[high] <= x) --high;
            arr[low] = arr[high];
            while (low < high && arr[low] >= x) ++low;
            arr[high] = arr[low];
        }
        arr[low] = x;
        return low;
    }

    int search(int[] arr, int i, int j, int k) {
        assert (i <= j);
        int q = partition(arr, i, j);
        if (q - i + 1 == k) {
            return arr[q];
        } else if (q - i + 1 < k) {
            return search(arr, q + 1, j, k - (q - i + 1));
        } else {
            return search(arr, i, q - 1, k);
        }
    }

    int get_nth(int arr[], int k) {
        if(k > arr.length)k = arr.length;
        return search(arr, 0, arr.length-1, k);
    }

}
