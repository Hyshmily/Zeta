package io.github.hyshmily.zeta.hotkeydetector.heavykeeper;

import java.lang.reflect.Field;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Manual measurement tool (NOT a test): hammers HeavyKeeper at fixed QPS for ~90s and prints
 * observed slot-sum (cur) values. It has no assertions and would inflate CI time, so it is
 * disabled from the surefire run. Run it manually when tuning HeavyKeeper decay.
 */
@Disabled("manual measurement tool, not a test — run manually when tuning HeavyKeeper decay")
class CurMeasurementTest {

  @Test
  void measureCurAtVariousQps() throws Exception {
    Field slotSumsField = HeavyKeeper.class.getDeclaredField("slotSums");
    slotSumsField.setAccessible(true);

    HeavyKeeper hk = new HeavyKeeper(100, 2048, 4, 0.9, 100, 5000, 3);
    String key = "hot-key";

    int[] qpsValues = { 100, 500, 1000 };
    long overallMaxCur = 0;

    System.out.printf("%n=== Measuring slot sums ===%n");

    for (int qps : qpsValues) {
      long maxCur = 0;
      int totalSec = 30;
      long totalOps = (long) qps * totalSec;

      for (long i = 0; i < totalOps; i++) {
        hk.addDirect(key, 1);

        int[] slotSums = (int[]) slotSumsField.get(hk);
        for (int cur : slotSums) {
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
