import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import jdk.incubator.vector.*;

public class ParallelPrimes {

    public static final String TEAM_NAME = "Daily Mammoth";
    public static final int MAX_VALUE = Integer.MAX_VALUE;
    public static final int ROOT_MAX = (int) Math.sqrt(MAX_VALUE);
    public static final int MAX_SMALL_PRIME = 1 << 20;
    public static final int N_THREADS = 115;
    public static final int ITERATIONS_PER_TASK = 2_000_000;
    public static ExecutorService pool;
    public static final VectorSpecies<Byte> SPECIES = ByteVector.SPECIES_PREFERRED;

    /**
     * Array takes isPrime array and using vectors, marks multiples of 2 and 3 as false
     * @param isPrime: the array used for sieve of eratosthenes
     * @param start: the number at which you start counting from when using isPrime array
     * @return number of multiples of 3 marked in isPrime array
     */
    public static int fillArray(byte[] isPrime, int start) {
        //get vector specifications
        int vectorStep = SPECIES.length();
        int bound = SPECIES.loopBound(isPrime.length);

        //used to count multiples of 3 within each array pattern
        //used by isPrime block when counting how many primes were found
        int firstCounter = 0;
        int secondCounter = 0;
        int thirdCounter = 0;

        //total number of multiples of 3 found after marking isPrime
        int totalCounter = 0;

        //when marking multiples of 3 sequentially within each set of numbers of size vector lane,
        // there are 3 patterns in which multiples of 3 can appear
        //each array stores this pattern for the vectors
        byte[] firstFiller = new byte[vectorStep];
        byte[] secondFiller = new byte[vectorStep];
        byte[] thirdFiller = new byte[vectorStep];

        for(int i = 0; i < vectorStep; i++) {
            //mark multiples of 2 as false
            if((start + i) % 2 == 0) {
                firstFiller[i] = 0;
                secondFiller[i] = 0;
                thirdFiller[i] = 0;
            } else {
                firstFiller[i] = 1;
                secondFiller[i] = 1;
                thirdFiller[i] = 1;
            }

            //mark multiples of 3 as false
            if((start + i) % 3 == 0) {
                firstFiller[i] = 0;
                if((start + i) % 2 != 0) firstCounter++;

            }
            if((start + vectorStep + i) % 3 == 0) {
                secondFiller[i] = 0;
                if((start + vectorStep + i) % 2 != 0) secondCounter++;

            }
            if((start + 2*vectorStep + i) % 3 == 0) {
                thirdFiller[i] = 0;
                if((start + 2*vectorStep + i) % 2 != 0) thirdCounter++;
            }
        }

        //create vectors from each array
        var firstVector = ByteVector.fromArray(SPECIES, firstFiller, 0);
        var secondVector = ByteVector.fromArray(SPECIES, secondFiller, 0);
        var thirdVector = ByteVector.fromArray(SPECIES, thirdFiller, 0);

        //iterate through isPrime array
        //use vectors to mark multiples of 2 and 3 as false
        int index = 0;
        while(index < bound) {
            firstVector.intoArray(isPrime, index);
            index += vectorStep;
            totalCounter += firstCounter;
            if(index < bound) {
                secondVector.intoArray(isPrime, index);
                index += vectorStep;
                totalCounter += secondCounter;
                if(index < bound) {
                    thirdVector.intoArray(isPrime, index);
                    index += vectorStep;
                    totalCounter += thirdCounter;
                }
            }
        }

        //iterate through remainders that could not be marked by vectors
        for(; index < isPrime.length; index++) {
            if(index % 2 == 0)
                isPrime[index] = 0;
            else if((start + index) % 3 == 0) {
                totalCounter++;
                isPrime[index] = 0;
            }
            else
                isPrime[index] = 1;
        }

        return totalCounter;

    }

    // Use the sieve of Eratosthenes to compute all prime numbers up
    // to max. The largest allowed value of max is MAX_SMALL_PRIME.
    public static int[] getSmallPrimes() {
        int max = ROOT_MAX;

        // check that the value max is in bounds, and throw an
        // exception if not
        if (max > MAX_SMALL_PRIME) {
            throw new RuntimeException("The value " + max + "exceeds the maximum small prime value (" + MAX_SMALL_PRIME + ")");
        }

        // isPrime[i] will be true if and only if i is
        // prime. Initially set isPrime[i] to true for all i >= 2.
        byte[] isPrime = new byte[max];
        int counter = max/2;
        counter -= fillArray(isPrime, 0);

        isPrime[2] = 1;
        isPrime[3] = 1;
        counter++; //used to adjust for 0-3 section of isPrime array

        // Apply the sieve of Eratosthenes to find primes. The
        // procedure iterates over values i = 2, 3,.... If isPrime[i]
        // == true, then i is a prime. When a prime value i is found,
        // set isPrime[j] = false for all multiples j of i. The
        // procedure terminates once we've examined all values i up to
        // Math.sqrt(max).


        int rootMax = (int) Math.sqrt(max);
        for (int i = 5; i < rootMax; i++) {
            if (isPrime[i] == 1) {
                for (int j = 5 * i; j < max; j += 2*i) {
                    if(isPrime[j] == 1) counter--;
                    isPrime[j] = 0;
                }
            }
        }

        // Count the number of primes we've found, and put them
        // sequentially in an appropriately sized array.
        //int count = trueCount(isPrime);

        int[] primes = new int[counter];
        int pIndex = 0;

        for (int i = 2; i < max; i++) {
            if (isPrime[i] == 1) {
                primes[pIndex++] = i;
                // pIndex++;
            }
        }

        return primes;
    }


    public static void optimizedPrimes(int[] primes) {

        //initialize thread pool to count primes
        pool = Executors.newFixedThreadPool(N_THREADS);

        // compute small prime values
        int[] smallPrimes = getSmallPrimes();
        int nPrimes = primes.length;

        // write small primes to primes
        int count = 0;
        int minSize = Math.min(nPrimes, smallPrimes.length);
        for (; count < minSize; count++) {
            primes[count] = smallPrimes[count];
        }

        // check if we've already filled primes, and return if so
        if (nPrimes == minSize) {
            return;
        }

        // Apply the sieve of Eratosthenes to find primes. This
        // procedure partitions the sieving task up into several
        // blocks, where each block isPrime stores boolean values
        // associated with ROOT_MAX consecutive numbers. Note that
        // partitioning the problem in this way is necessary because
        // we cannot create a boolean array of size MAX_VALUE.

        List<Future<int[]>> results = new ArrayList<>((MAX_VALUE/ITERATIONS_PER_TASK));

        for (long curBlock = ROOT_MAX; curBlock < MAX_VALUE && count < nPrimes; curBlock += ITERATIONS_PER_TASK) {
            results.add(pool.submit(new PrimeBlockTask((int) curBlock, smallPrimes, ITERATIONS_PER_TASK)));
        }

        for(Future<int[]> result: results) {
            try {
                int[] newPrimes = result.get();
                for(int i = 0; i < newPrimes.length; i++)
                    primes[count++] = newPrimes[i];
            } catch (Exception e) {}

        }

        pool.shutdown();
        try {
            pool.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (Exception e) {}
    }
}

class PrimeBlockTask implements Callable<int[]> {
    int iterations; //number of numbers in isPrime array to iterate through
    int start; //starting number from which to find primes
    int[] smallPrimes; //small primes found before used for reference
    byte[] isPrime; //isPrime array to mark and use to find primes
    public PrimeBlockTask(int start, int[] smallPrimes, int iterations) {
        this.start = start;
        this.smallPrimes = smallPrimes;
        this.iterations = iterations;
        this.isPrime = new byte[iterations];
    }

    public int[] call() {
        //stores number of primes found
        //we can divide it 2 since all even numbers are not prime
        int counter = iterations/2;

        //fill isPrime array with multiples of 2 and 3 already marked as false
        //subtract multiples of 3 from counter
        counter -= ParallelPrimes.fillArray(isPrime, start);

        //iterate through small primes and mark multiples of each prime as false
        //providing credit to Prof. Rosenbaum for this chunk of code as it was adapted from his baseline implementation
        for (int p : smallPrimes) {
            if(p == 2 || p == 3) continue; //we can skip 2 and 3 since they were already marked as false
            // find the next number >= start that is a multiple of p
            int i = (start % p == 0) ? start : p * (1 + start / p);
            i -= start;

            while (i < isPrime.length) {
                if(isPrime[i] == 1) counter--; //subtract counter as new prime was found
                isPrime[i] = 0;
                i += p;
            }
        }


        //store primes found
        int[] primes = new int[counter];
        int currentIndex = 0;

        //iterate through isPrime and store found primes in array
        for (int i = 0; i < isPrime.length; i++) {
            if (isPrime[i] == 1) {
                primes[currentIndex++] = (int) start + i;
            }
        }

        //return array of found primes
        return primes;
    }
}

