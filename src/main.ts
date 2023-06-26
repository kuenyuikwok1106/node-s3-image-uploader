import * as fs from "fs";
import sharp from 'sharp';
const convert = require('heic-convert');
import { v4 as uuidv4, validate as uuidValidate } from 'uuid';
import { promisify } from 'util';
import { S3Client, PutObjectCommand, ListObjectsV2Command } from '@aws-sdk/client-s3';
require('dotenv').config();

const parse = async(ABSOLUTE_PATH, awsS3Client) => {
  const isDirectory = fs.lstatSync(ABSOLUTE_PATH).isDirectory();
  
  if (isDirectory) {
    const directories = (await fs.promises.readdir(ABSOLUTE_PATH)).filter((dir) => dir !== '.DS_Store');
    if (directories.length > 0) {
      for (let i = 0; i < directories.length; i ++) {
        const path = ABSOLUTE_PATH + `/${directories[i]}`;
        parse(path, awsS3Client);
      }
    }
  } else {
    const pathSegment = ABSOLUTE_PATH.split('/');
    const { width, height, format } = await sharp(ABSOLUTE_PATH).metadata();
    
    const imageNameWithExtension = pathSegment[pathSegment.length - 1];
    let imageName = imageNameWithExtension.split('.')[0];
    const isUuid = uuidValidate(imageName);
    if(!isUuid) imageName = uuidv4();
    imageName = imageName + '.jpeg';
    
    const newPathSegment = [...pathSegment];
    newPathSegment[newPathSegment.length - 1] = imageName;
    if (imageNameWithExtension !== imageName) {
      fs.rename(
        ABSOLUTE_PATH,
        newPathSegment.join('/'),
        () => { console.log(`from ${pathSegment} to ${newPathSegment}`)}
      );
    }
    
    // check image existence; do nothing if it exist; or else upload the image;
    const cmd = new ListObjectsV2Command({
      Bucket: 'my-trip-bucket',
      Prefix: newPathSegment.slice(newPathSegment.length - 3).join('/'),
    });
    const { Contents } = await awsS3Client.send(cmd);
  
    if (Contents === undefined) {
      let image;
      if (format === 'heif') {
        image = await convert({
          buffer: await promisify(fs.readFile)(ABSOLUTE_PATH), // the HEIC file buffer
          format: 'JPEG',      // output format
          quality: 1           // the jpeg compression quality, between 0 and 1
        });
      } else {
        image = await sharp(ABSOLUTE_PATH).jpeg({
          quality: 100,
          chromaSubsampling: '4:4:4'
        }).toBuffer();
      }
      const cmd = new PutObjectCommand({
        'Bucket': process.env.S3_BUCKET_NAME,
        'Body': image,
        'Key': newPathSegment.slice(newPathSegment.length - 3).join('/'),
        'Metadata': { 'width': width.toString(), 'height': height.toString() },
      });
      const response = await awsS3Client.send(cmd);
      console.log(response);
    }
  }
}

const s3Client = (path, accessKeyId, secretAccessKey, region) => {
  const client = new S3Client({
    credentials: { accessKeyId, secretAccessKey },
    region,
    apiVersion: '2006-03-01',
  });
  return parse(path, client)
}

s3Client(
  process.env.ABSOLUTE_PATH,
  process.env.S3_ACCESS_KEY_ID,
  process.env.S3_SECRET_ACCESS_KEY,
  process.env.S3_REGION,
)
