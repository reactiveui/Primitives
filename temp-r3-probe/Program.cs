using System;
using R3;

public sealed class Program
{
    public static void Main()
    {
        var subject = new Subject<int>();
        var disposables = new System.IDisposable[8];
        try
        {
            for (var i=0; i<8; i++)
            {
                disposables[i] = subject.Subscribe(new IntObserver());
            }
            Console.WriteLine("created");
        }
        catch (Exception ex)
        {
            Console.WriteLine(ex);
            return;
        }

        for (var i=0; i<8; i++)
        {
            disposables[i].Dispose();
        }
        Console.WriteLine("disposed");
    }

    private sealed class IntObserver : Observer<int>
    {
        protected override void OnNextCore(int value) {}
        protected override void OnErrorResumeCore(Exception error) {}
        protected override void OnCompletedCore(Result result) {}
    }
}
