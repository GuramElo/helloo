#!/usr/bin/env python3
import subprocess
import argparse
import sys
import shlex

def run_ffmpeg_command(command_list):
    """Executes an FFmpeg command using subprocess."""
    
    # Print the command for the user to see
    # shlex.join() correctly formats the command for display
    print(f"Executing command:\n{shlex.join(command_list)}\n")
    
    try:
        # Run the command
        # check=True will raise an error if ffmpeg fails
        # capture_output=True will hide ffmpeg's log unless an error occurs
        result = subprocess.run(
            command_list, 
            check=True, 
            capture_output=True, 
            text=True
        )
        print("Success! Output file created.")
        print(result.stdout)
        
    except subprocess.CalledProcessError as e:
        # If ffmpeg fails, print its error log
        print(f"Error executing FFmpeg:")
        print("STDOUT:", e.stdout)
        print("STDERR:", e.stderr)
        
    except FileNotFoundError:
        print("Error: 'ffmpeg' command not found.")
        print("Please make sure FFmpeg is installed and in your system's PATH.")
        sys.exit(1)

def main():
    parser = argparse.ArgumentParser(
        description="Syncs an audio track from movie2 to movie1 with a time offset."
    )
    
    parser.add_argument("movie1", help="Path to the primary movie file (input 1)")
    parser.add_argument("movie2", help="Path to the secondary movie file (input 2)")
    parser.add_argument(
        "offset", 
        type=float, 
        help="The time offset in seconds. Positive to seek, negative to delay."
    )
    
    parser.add_argument(
        "-a", "--audio_stream", 
        default="a:0", 
        help="The audio stream specifier for movie2 (e.g., 'a:0', 'a:1'). Default is 'a:0'."
    )
    
    parser.add_argument(
        "-o", "--output", 
        default="output.mp4", 
        help="The name of the output file. Default is 'output.mp4'."
    )

    args = parser.parse_args()

    # --- Build the FFmpeg command ---
    
    # Start with the ffmpeg executable
    ffmpeg_command = ["ffmpeg"]

    # Handle the offset logic
    if args.offset >= 0:
        # POSITIVE N: Seek in movie2
        # -i movie1 -ss [N] -i movie2
        ffmpeg_command.extend(["-i", args.movie1])
        ffmpeg_command.extend(["-ss", str(args.offset)])
        ffmpeg_command.extend(["-i", args.movie2])
    else:
        # NEGATIVE N: Delay movie2
        # -i movie1 -itsoffset [abs(N)] -i movie2
        ffmpeg_command.extend(["-i", args.movie1])
        ffmpeg_command.extend(["-itsoffset", str(abs(args.offset))])
        ffmpeg_command.extend(["-i", args.movie2])

    # Add the mapping and copy logic
    # -map 0 (all streams from movie1)
    # -map 1:a:0 (specified audio stream from movie2)
    # -c copy (copy all streams without re-encoding)
    ffmpeg_command.extend(["-map", "0"])
    ffmpeg_command.extend(["-map", f"1:{args.audio_stream}"])
    ffmpeg_command.extend(["-c", "copy"])

    # Add the output file
    ffmpeg_command.append(args.output)
    
    # Run the command
    run_ffmpeg_command(ffmpeg_command)

if __name__ == "__main__":
    main()