package com.tencent.angel.graph.util;

public class CompactSampler {
    public static int randomSelect(float[] accSumWeights, int beginPos, int endPos) {
        float limitBegin = beginPos == 0 ? 0 : accSumWeights[beginPos - 1];
        float limitEnd = accSumWeights[endPos];
        float r = (float) Math.random() * (limitEnd - limitBegin) + limitBegin;

        int low = beginPos, high = endPos, mid = 0;
        boolean finish = false;
        while (low <= high && !finish) {
            mid = (low + high) / 2;
            float intervalBegin = mid == 0 ? 0 : accSumWeights[mid - 1];
            float intervalEnd = accSumWeights[mid];
            if (intervalBegin <= r && r < intervalEnd) {
                finish = true;
            } else if (intervalBegin > r) {
                high = mid - 1;
            } else if (intervalEnd <= r) {
                low = mid + 1;
            }
        }

        return mid;
    }
}
