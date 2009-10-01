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
 * Master.groovy
 * Created on Apr 27, 2009
 */

package mandelbrot

import org.gruple.Space
import org.gruple.SpaceService
import java.awt.Color
import java.awt.Graphics
import java.awt.Image
import java.awt.Frame
import java.awt.event.MouseListener
import java.awt.event.MouseMotionListener
import java.awt.event.MouseEvent


/**
* Implementation of a mandelbrot computation program using a distributed worker
* pattern and the demo tuplespace package.
*
* ACKNOWLEDGEMENT:	This code was adapted from the book "JavaSpaces Principles,
*					Patterns, and Practice" by Freeman, Hupfer, and Arnold,
*					Addison-Wesley, 1999.
*
 * @author Vanessa Williams <vanessa@fridgebuzz.com>
 */
class Master 
    implements Runnable
{
    private Space space

    private int workers         // number of worker threads
    private int lines = 0       // # of scan lines per task

    int xsize, ysize            // dimensions of window
    private Long jobId          // id for this computation
    private int tasks           // number of tasks to generate

    private Thread master

    // initial region for which Mandelbrot is being computed
    double x1 = -2.25d
    double x2 =  3.0d
    double y1 = -1.8d
    double y2 =  3.3d

    int progress        // number of scan lines
    boolean drag = false
    boolean done = false


    // off-screen buffer and graphics
    Image offscreen
    Graphics offg

    private MandelbrotFrame frame

	public Master(int tasks, int workers, int width, int height) {
		this.tasks = tasks
		this.workers = workers

        space = SpaceService.getSpace("mandelbrot");
        xsize = width
        ysize = height

		frame = new MandelbrotFrame(width, height, this)

        lines = ysize / tasks;  // scan lines per task

        // create offscreen buffer
        offscreen = frame.createImage(xsize, ysize)
        offg = offscreen.graphics
        offg.color = Color.black
        offg.fillRect(0, 0, xsize, ysize)


        // spawn threads to handle computations

        if (!master) {
            master = new Thread(this)
            master.start()
        }

        (0..<workers).each {
        	Thread worker = new Thread(new Worker())
        	worker.start()
        }

	}

	static void main(args) {

		int tasks = Integer.parseInt(args[0])
		int workers = Integer.parseInt(args[1])
		int width = Integer.parseInt(args[2])
		int height = Integer.parseInt(args[3])

		new Master(tasks, workers, width, height)
	}

    void run() {
        generateTasks()
        collectResults()
    }

    protected void generateTasks() {


        jobId = System.currentTimeMillis() + Math.round(Math.random()*32765)
        Map task = ["jobId":jobId, "x1":x1, "x2":x2, "y1":y1, "y2":y2, width:xsize, height:ysize, "lines":lines]


        0.step(ysize-1, lines) { i ->
            task.put("start", i)
            println "Master writing task ${task['start']} for job ${task['jobId']}"
            space.put(task)
        }
    }

    private void collectResults() {
        while (true) {
            while (done) {
                try {
                    println "Master done; waiting for new job."
                    Thread.sleep(500)
                } catch (InterruptedException e) {
                    e.printStackTrace()
                }
            }

            Map template = ["jobId":jobId, "start":null, "points":null]
            def result

            try {
                result = space.take(template)
                println"Master collected result ${result['start']} for job ${result['jobId']}"
                display(result)
                progress += lines
            } catch (Exception e) {
                e.printStackTrace();
            }

            if (progress == ysize) {
                done = true
                frame.repaint()
            }
        }
    }

    private Color getPixelColor(int val) {

        Color color
        switch(val) {
            case 100        : color = new Color(0,0,0); break;
            case 91..<100   : color = new Color(val*2,0,(val-90)*25); break;
            case 81..90     : color = new Color(val*2,0,0); break;
            case 61..80     : color = new Color(val*3,0,val); break;
            case 21..60     : color = new Color(val*4,0,val*2); break;
            case 11..20     : color = new Color(val*5,0,val*10); break;
            default         : color = new Color(0,0,val*20); break;
        }
        return color
    }

    private void display(Map result) {
        ArrayList points = result["points"] as ArrayList
        def l = result["start"]
        for(j in 0..< lines) {
            for(int k in 0..< xsize) {
                ArrayList row = points.getAt(k) as ArrayList
                def n = row.getAt(j)
                Color pixelColor = getPixelColor(n as int)
                offg.color = pixelColor
                offg.fillRect(k, l as int, 1, 1)

            }
            l++
        }
        frame.repaint()
    }

}

class MandelbrotFrame extends Frame
    implements MouseListener, MouseMotionListener
{
    Master master

    public MandelbrotFrame(int width, int height, Master parent) {
        super("Mandelbrot Viewer")
        master = parent;

        setSize(width, height)
        // set up listeners
        addMouseListener(this)
        addMouseMotionListener(this)
        show()
    }

    public void update( Graphics g ) {
        if (!master.drag) {
            paint(g)
        }
    }

    public void paint( Graphics g ) {
        if (!master.drag) {
            if (master.done) {
                g.drawImage(master.offscreen,0,0,this)
            }
            else {
                g.drawImage(master.offscreen,0,0,this)
                g.color = Color.white
                int xsize = master.xsize
                g.drawRect(xsize/4 as int, 10, xsize/2 as int, 5)
                g.fillRect(xsize/4 as int, 11, (master.progress*(xsize/2))/master.ysize as int, 4)
            }
        }
    }

    public void mouseReleased(MouseEvent e) {
        int x = e.x;
        int y = e.y;
        if (master.done) {
            double x1 = master.x1
            double x2 = master.x2
            double y1 = master.y1
            double y2 = master.y2
            x1 += ((x as float) / (master.xsize as float)) * x2
            y1 += ((y as float) / (master.ysize as float)) * y2
            x2 /= 2.0
            y2 /= 2.0
            x1 -= x2 / 2.0
            y1 -= y2 / 2.0
            master.done = false
            master.drag = false
            master.offg.color = Color.black
            master.offg.fillRect(0, 0, master.xsize, master.ysize)
            master.progress = 0
            repaint()
            master.generateTasks()
        }
    }

    public void mouseDragged(MouseEvent e) {
        int x = e.x
        int y = e.y
        if (master.done) {
            master.drag=true
            Graphics g=getGraphics()
            g.drawImage(master.offscreen,0,0,this);
            g.color = Color.white
            int xsize = master.xsize
            int ysize = master.ysize
            g.drawRect(x-xsize/4 as int,y-ysize/4 as int,xsize/2 as int,ysize/2 as int)
        }
    }

    public void mousePressed(MouseEvent e) {}
    public void mouseClicked(MouseEvent e) {}
    public void mouseEntered(MouseEvent e) {}
    public void mouseExited(MouseEvent e) {}
    public void mouseMoved(MouseEvent e) {}


}



