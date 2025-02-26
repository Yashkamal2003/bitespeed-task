CREATE TABLE IF NOT EXISTS contacts (
  id SERIAL PRIMARY KEY,
  "phoneNumber" VARCHAR(255),
  email VARCHAR(255),
  "linkedId" INTEGER,
  "linkPrecedence" VARCHAR(255) NOT NULL,
  "createdAt" TIMESTAMP WITH TIME ZONE NOT NULL,
  "updatedAt" TIMESTAMP WITH TIME ZONE NOT NULL,
  "deletedAt" TIMESTAMP WITH TIME ZONE
);
