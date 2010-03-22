/*
 *    Copyright 2009 Vanessa Williams
 * 
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 * 
 *        http://www.apache.org/licenses/LICENSE-2.0
 * 
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

/*
 * Worker.groovy
 * Created on Apr 29, 2009
 */

package mandelbrot

import org.gruple.Space
import org.gruple.Spaces
import org.gruple.Transaction

/**
 *
 * @author Vanessa Williams <vanessa@fridgebuzz.com>
 */
class Worker implements Runnable {
    
    public void run() {
        Space space = Spaces["mandelbrot"]

        Map template = createTaskTemplate()
        Map task

        String threadName = Thread.currentThread().getName()
        while(true) {
            ArrayList points

            Transaction txn = new Transaction()
            task = space.take(template, Space.WAIT_FOREVER, txn)            
            println "Worker $threadName got task ${task['start']} for job ${task['jobId']}"

            points = calculateMandelbrot(task)
            Map result = createResult(task['jobId'], task['start'], points)
            println "Worker $threadName writing result for task ${result['start']} for job ${result['jobId']}"

            space.put(result, Space.FOREVER, txn)
            txn.commit()
        }
    }

    public static ArrayList calculateMandelbrot(Map task) {
        double x, y, xx, a, b
        int start = task['start'] as int
        int lines = task['lines'] as int
        int end = start + lines
        int width = task['width'] as int
        int height = task['height'] as int

        double da = task['x2']/width
        double db = task['y2']/height

        b = task['y1'] as double

        ArrayList points = new ArrayList()
        for (i in 0..<start) {
            b = b + db
        }

        int k = 0

        for (i in start..< end) {
            a = task['x1'] as double
            for (j in 0..<width) {
                byte n = 0
                x = 0.0
                y = 0.0
                while ( (n < 100) && ( (x*x)+(y*y) < 4.0) ) {
                    xx = x * x - y * y + a
                    y = 2 * x * y + b
                    x = xx
                    n++
                }
                ArrayList rows = points.getAt(j)
                if (rows != null)
                    rows.putAt(k, n)
                else {
                    rows = new ArrayList()
                    rows.putAt(k, n)
                    points.putAt(j, rows)
                }

                a = a + da
            }
            b = b + db
            k++
        }
        return points
    }

    private Map createTaskTemplate() {
		return [jobId:null, x1:null, y1:null, x2:null, y2:null, start:null, width:null, height:null, lines:null]

    }

    private Map createResult(jobId, start, points) {
    	return ["jobId":jobId, "start":start, "points":points]
    }
	
}

