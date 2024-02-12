import prrocr
import argparse

langs = prrocr.ocr.get_available_langs()
parser = argparse.ArgumentParser(description='Download images for a specific city.')
parser.add_argument('--url', type=str, required=True, help='url for an image')
args = parser.parse_args()

ocr = prrocr.ocr(lang="ko")
output = ocr(args.url)
print(output)