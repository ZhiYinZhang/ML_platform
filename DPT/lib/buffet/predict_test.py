import torch

import numpy as np
from torchvision import transforms

import os.path


# img preprocessing
import requests as req
from PIL import Image
from io import BytesIO
import pickle
import json

# def buffet_infer(model, url, trans, class_names):
#     response = req.get(url)
#     image = Image.open(BytesIO(response.content))
#     inputs = img_trans(image)
#     inputs = inputs.unsqueeze(0)
#     with torch.no_grad():
#         model.eval()
#         outputs = model(inputs)
#         probs = torch.softmax(outputs, dim=1)
#         probs = probs.view(-1)
#         top5 = np.argsort(probs.numpy())[-5:].tolist()
#         top5.reverse()

#     return json.dumps(list(zip(probs.numpy()[top5].tolist(), np.array(class_names)[top5].tolist())))


model_path = os.path.join(os.path.dirname(__file__), 'model_importance_epoch10_1009.pkl')
model = torch.load(model_path)

img_trans = transforms.Compose([
        transforms.Resize(256),
        transforms.CenterCrop(224),
        transforms.ToTensor(),
        transforms.Normalize([0.485, 0.456, 0.406], [0.229, 0.224, 0.225])
    ])

class_path = os.path.join(os.path.dirname(__file__), 'class_names.pkl')
class_names = pickle.load(open(class_path, 'rb'))

def buffet_infer(url):
    img_trans = transforms.Compose([
        transforms.Resize(256),
        transforms.CenterCrop(224),
        transforms.ToTensor(),
        transforms.Normalize([0.485, 0.456, 0.406], [0.229, 0.224, 0.225])
    ])
    headers = {"User-Agent":"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/45.0.2454.101 Safari/537.36"}
    response = req.get(url, headers=headers, stream=True)
    image = Image.open(BytesIO(response.content))
    image = image.convert("RGB")
    inputs = img_trans(image)
    inputs = inputs.unsqueeze(0)
    with torch.no_grad():
        model.eval()
        outputs = model(inputs)
        probs = torch.softmax(outputs, dim=1)
        probs = probs.view(-1)
        top5 = np.argsort(probs.numpy())[-5:].tolist()
        top5.reverse()

    # return json.dumps(list(zip(probs.numpy()[top5].tolist(), np.array(class_names)[top5].tolist())))
    return json.dumps(list(zip(np.array(class_names)[top5].tolist(), probs.numpy()[top5].tolist())))


if __name__ == "__main__":

    url = 'https://timgsa.baidu.com/timg?image&quality=80&size=b9999_10000&sec=1538020600708&di=4fc3d891a5012ef33784dd8406cf6a13&imgtype=0&src=http%3A%2F%2Fwww.shang360.com%2Fupload%2Farticle%2F20160919%2F96043540091474282247.jpg'

    model_path = 'model_importance_epoch25.pkl'
    model = torch.load(model_path)
    img_trans = transforms.Compose([
            transforms.Resize(256),
            transforms.CenterCrop(224),
            transforms.ToTensor(),
            transforms.Normalize([0.485, 0.456, 0.406], [0.229, 0.224, 0.225])
        ])
    class_names = pickle.load(open('class_names.pkl', 'rb'))

    response = req.get(url)
    image = Image.open(BytesIO(response.content))
    inputs = img_trans(image)
    inputs = inputs.unsqueeze(0)
    with torch.no_grad():
        model.eval()
        outputs = model(inputs)
        probs = torch.softmax(outputs, dim=1)
        probs = probs.view(-1)
        top5 = np.argsort(probs.numpy())[-5:].tolist()
        top5.reverse()

    print(probs.numpy()[top5].tolist())
    print(np.array(class_names)[top5].tolist())
