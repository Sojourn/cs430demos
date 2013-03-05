using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

/// <summary>
/// An example which uses a Parallel.For loop to search for a line
/// in a long document.
/// </summary>
public class ParallelForDemo
{

    static void Main(string[] args)
    {
        System.Random random = new System.Random(0);

        // Create a random document full of integers
        string[] document = new string[64 * 1024];
        for(int i = 0; i < document.Length; i++)
        {
            // Create a random line
            StringBuilder builder = new StringBuilder();
            for(int j = 0; j < 64; j++)
                builder.Append(random.Next());
            
            document[i] = builder.ToString();
        }

        // Search for a random line in the document
        long lineNumber = 0;
        string key = document[random.Next() % document.Length];
        ParallelLoopResult result = Parallel.For(0, document.LongLength, (long i, ParallelLoopState loop) =>
            {

                // Compare the line with the key
                if (document[i] == key)
                {
                    lineNumber = i;
                    loop.Stop();
                }

                return;
            });

        Console.WriteLine("Found key on line {0}.", lineNumber);
        Console.WriteLine("Press any key to continue...");
        Console.ReadKey();
    }
}
