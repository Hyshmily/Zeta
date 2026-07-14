package io.github.hyshmily.zeta.hotkeydetector.heavykeeper;

import org.junit.jupiter.api.Test;
import java.lang.reflect.Field;

/**
 * Measure slot-sum (cur) values that occur in practice under load.
 */
class CurMeasurementTest {

    @Test
    void measureCurAtVariousQps() throws Exception {
        Field slotSumsField = HeavyKeeper.class.getDeclaredField("slotSums");
        slotSumsField.setAccessible(true);

        HeavyKeeper hk = new HeavyKeeper(100, 2048, 4, 0.9, 100, 5000, 3);
        String key = "hot-key";

        int[] qpsValues = {100, 500, 1000};
        long overallMaxCur = 0;

        System.out.printf("%n=== Measuring slot sums ===%n");

        for (int qps : qpsValues) {
            long maxCur = 0;
            int totalSec = 30;
            long totalOps = (long) qps * totalSec;

            for (long i = 0; i < totalOps; i++) {
                hk.addDirect(key, 1);

                long[] slotSums = (long[]) slotSumsField.get(hk);
                for (long cur : slotSums) {
                    if (cur > maxCur) maxCur = cur;
                }

                if (i > 0 && i % ((long) qps * 20) == 0) {
                    hk.fading();
                }
            }

            if (maxCur > overallMaxCur) overallMaxCur = maxCur;
            System.out.printf("  %4d qps × %ds: maxSlotSum=%,d%n", qps, totalSec, maxCur);
        }

        System.out.printf("  Overall maxSlotSum=%,d%n", overallMaxCur);
    }
}
