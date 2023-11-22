import { MongoClient } from "mongodb";
import { chunk } from "lodash";

type DetailsData = {
  fields: { page_number: number; name: string }[]; // Replace 'any' with a more specific type if possible
};

// Constants
const BATCH_SIZE = 3;

// Environment Variables
const PDF_OTTER_API_KEY = process.env.PDF_OTTER_API_KEY;
const PDF_OTTER_ENDPOINT = process.env.PDF_OTTER_ENDPOINT;
const MONGO_CONN_STR = process.env.MONGO_CONN_STR;
const MONGO_DATABASE = process.env.MONGO_DATABASE;

const COLLECTION_NAME = "pdfOtter";

if (!PDF_OTTER_API_KEY) {
  throw new Error("Missing PDF OTTER API KEY");
}

if (!PDF_OTTER_ENDPOINT) {
  throw new Error("Missing PDF Otter Endpoint");
}

if (!MONGO_CONN_STR) {
  throw new Error("Missing MONGO_CONN_STR");
}

if (!MONGO_DATABASE) {
  throw new Error("Missing MONGO_DATABASE");
}

type NonDetailedTemplate = {
  id: string;
  name: string;
};
const isTemplate = (obj: any): obj is NonDetailedTemplate => {
  return typeof obj.id === "string" && typeof obj.name === "string";
};

// Types
interface PDFTemplate {
  id: string;
  name: string;
  fields?: Array<{ page_number: number; name: string }>;
}

// MongoDB Connection
const client = new MongoClient(MONGO_CONN_STR);

// Fetching PDF Templates in Batches
async function fetchPDFTemplatesInBatches(
  batchSize: number
): Promise<PDFTemplate[]> {
  const response = await fetch(`${PDF_OTTER_ENDPOINT}/pdf_templates`, {
    headers: {
      Authorization: `Basic ${Buffer.from(`${PDF_OTTER_API_KEY}:`).toString(
        "base64"
      )}`,
    },
  });

  const resp = (await response.json()) as unknown;

  if (
    !resp ||
    !Array.isArray(resp) ||
    resp.some((template: unknown) => !isTemplate(template))
  ) {
    console.error("bad data:", resp);
    throw new TypeError("Recieved non template data");
  }
  const chunks: NonDetailedTemplate[][] = chunk(resp, batchSize);

  const templates = await Promise.all(
    chunks.map(async (chunk) => {
      const detailedTemplates = await Promise.all(
        chunk.map(fetchTemplateDetails)
      );
      return detailedTemplates;
    })
  ).then((results) => results.flat());

  return templates;
}

async function fetchTemplateDetails(template: NonDetailedTemplate) {
  const detailsResponse = await fetch(
    `${PDF_OTTER_ENDPOINT}/pdf_templates/${template.id}`,
    {
      headers: {
        Authorization: `Basic ${Buffer.from(`${PDF_OTTER_API_KEY}:`).toString(
          "base64"
        )}`,
      },
    }
  );

  const detailsData = (await detailsResponse.json()) as DetailsData;

  if (!detailsData?.fields) {
    console.warn(`Template has no fields ${template.id}`, detailsData);
  }

  console.log(detailsData.fields)
  return { ...template, fields: detailsData.fields };
}

// Storing Data in MongoDB
async function storeTemplatesInMongo(templates: PDFTemplate[]) {
  const db = client.db(MONGO_DATABASE);
  const collection = db.collection(COLLECTION_NAME);

  await Promise.all(
    templates.map((template) =>
      collection.updateOne(
        { id: template.id },
        { $set: template },
        { upsert: true }
      )
    )
  );
}

// Main Function
async function main() {
  try {
    await client.connect();
    console.log("Connected successfully to MongoDB server");

    const templates = await fetchPDFTemplatesInBatches(BATCH_SIZE);
    await storeTemplatesInMongo(templates);

    console.log("Templates have been fetched and stored successfully");
  } catch (error) {
    console.error("An error occurred:", error);
  } finally {
    await client.close();
    console.log("MongoDB connection closed");
  }
}

main();
