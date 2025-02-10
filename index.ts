

import  express, { Request, Response } from 'express';
import { Sequelize, Model, DataTypes, Op, Transaction } from 'sequelize';

const sequelize = new Sequelize({
  dialect: 'postgres',
  host: process.env.DB_HOST,
  port: parseInt(process.env.DB_PORT || '5432'),
  username: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_NAME,
  logging: false,
});

class Contact extends Model {
  public id!: number;
  public phoneNumber!: string | null;
  public email!: string | null;
  public linkedId!: number | null;
  public linkPrecedence!: 'primary' | 'secondary';
  public createdAt!: Date;
  public updatedAt!: Date;
  public deletedAt!: Date | null;
}

Contact.init(
  {
    id: {
      type: DataTypes.INTEGER,
      autoIncrement: true,
      primaryKey: true,
    },
    phoneNumber: {
      type: DataTypes.STRING,
      allowNull: true,
    },
    email: {
      type: DataTypes.STRING,
      allowNull: true,
    },
    linkedId: {
      type: DataTypes.INTEGER,
      allowNull: true,
    },
    linkPrecedence: {
      type: DataTypes.ENUM('primary', 'secondary'),
      allowNull: false,
    },
    createdAt: {
      type: DataTypes.DATE,
      allowNull: false,
    },
    updatedAt: {
      type: DataTypes.DATE,
      allowNull: false,
    },
    deletedAt: {
      type: DataTypes.DATE,
      allowNull: true,
    },
  },
  {
    sequelize,
    modelName: 'Contact',
    tableName: 'contacts',
    timestamps: true,
    paranoid: true,
  }
);

const app = express();
app.use(express.json());

app.post('/identify', async (req: Request, res: Response) => {
  const { email, phoneNumber } = req.body;

  if (!email && !phoneNumber) {
    return res.status(400).json({ error: 'At least one of email or phoneNumber is required.' });
  }

  const transaction = await sequelize.transaction();
  try {
    const whereClause: any = { deletedAt: null };
    const orConditions = [];
    if (email !== undefined && email !== null) {
      orConditions.push({ email });
    }
    if (phoneNumber !== undefined && phoneNumber !== null) {
      orConditions.push({ phoneNumber });
    }
    whereClause[Op.or] = orConditions;

    const existingContacts = await Contact.findAll({
      where: whereClause,
      transaction,
    });

    const primaryContacts = new Set<Contact>();
    for (const contact of existingContacts) {
      if (contact.linkPrecedence === 'secondary') {
        const primaryContact = await Contact.findByPk(contact.linkedId, { transaction });
        if (!primaryContact) {
          throw new Error(`Secondary contact ${contact.id} has invalid linkedId ${contact.linkedId}`);
        }
        primaryContacts.add(primaryContact);
      } else {
        primaryContacts.add(contact);
      }
    }

    if (primaryContacts.size === 0) {
      const newContact = await Contact.create({
        email: email || null,
        phoneNumber: phoneNumber || null,
        linkedId: null,
        linkPrecedence: 'primary',
        createdAt: new Date(),
        updatedAt: new Date(),
      }, { transaction });

      await transaction.commit();

      return res.json({
        contact: {
          primaryContatctId: newContact.id,
          emails: newContact.email ? [newContact.email] : [],
          phoneNumbers: newContact.phoneNumber ? [newContact.phoneNumber] : [],
          secondaryContactIds: [],
        }
      });
    }

    const primariesArray = Array.from(primaryContacts);
    const mainPrimary = primariesArray.reduce((oldest, current) => {
      return oldest.createdAt < current.createdAt ? oldest : current;
    });

    const otherPrimaries = primariesArray.filter(p => p.id !== mainPrimary.id);
    for (const oldPrimary of otherPrimaries) {
      await oldPrimary.update({
        linkedId: mainPrimary.id,
        linkPrecedence: 'secondary',
        updatedAt: new Date(),
      }, { transaction });

      await Contact.update(
        { linkedId: mainPrimary.id },
        { where: { linkedId: oldPrimary.id }, transaction }
      );
    }

    const clusterContacts = await Contact.findAll({
      where: {
        [Op.or]: [
          { id: mainPrimary.id },
          { linkedId: mainPrimary.id }
        ],
        deletedAt: null,
      },
      order: [['createdAt', 'ASC']],
      transaction,
    });

    const clusterEmails = clusterContacts.map(c => c.email).filter(e => e !== null) as string[];
    const clusterPhones = clusterContacts.map(c => c.phoneNumber).filter(p => p !== null) as string[];

    let createSecondary = false;
    if (email !== null && email !== undefined && !clusterEmails.includes(email)) {
      createSecondary = true;
    }
    if (phoneNumber !== null && phoneNumber !== undefined && !clusterPhones.includes(phoneNumber)) {
      createSecondary = true;
    }

    if (createSecondary) {
      await Contact.create({
        email: email || null,
        phoneNumber: phoneNumber || null,
        linkedId: mainPrimary.id,
        linkPrecedence: 'secondary',
        createdAt: new Date(),
        updatedAt: new Date(),
      }, { transaction });
    }

    await transaction.commit();

    const updatedCluster = await Contact.findAll({
      where: {
        [Op.or]: [
          { id: mainPrimary.id },
          { linkedId: mainPrimary.id }
        ],
        deletedAt: null,
      },
      order: [['createdAt', 'ASC']],
    });

    const emailsSet = new Set<string>();
    const phoneNumbersSet = new Set<string>();
    const secondaryContactIds: number[] = [];

    for (const contact of updatedCluster) {
      if (contact.id === mainPrimary.id) {
        if (contact.email) emailsSet.add(contact.email);
        if (contact.phoneNumber) phoneNumbersSet.add(contact.phoneNumber);
      } else {
        secondaryContactIds.push(contact.id);
        if (contact.email) emailsSet.add(contact.email);
        if (contact.phoneNumber) phoneNumbersSet.add(contact.phoneNumber);
      }
    }

    const emailsArray = Array.from(emailsSet);
    const phoneNumbersArray = Array.from(phoneNumbersSet);

    if (mainPrimary.email && emailsArray[0] !== mainPrimary.email) {
      emailsArray.unshift(mainPrimary.email);
      const uniqueEmails = Array.from(new Set(emailsArray));
      emailsArray.length = 0;
      emailsArray.push(...uniqueEmails);
    }

    if (mainPrimary.phoneNumber && phoneNumbersArray[0] !== mainPrimary.phoneNumber) {
      phoneNumbersArray.unshift(mainPrimary.phoneNumber);
      const uniquePhones = Array.from(new Set(phoneNumbersArray));
      phoneNumbersArray.length = 0;
      phoneNumbersArray.push(...uniquePhones);
    }

    res.json({
      contact: {
        primaryContatctId: mainPrimary.id,
        emails: emailsArray,
        phoneNumbers: phoneNumbersArray,
        secondaryContactIds: secondaryContactIds,
      }
    });

  } catch (error) {
    await transaction.rollback();
    console.error(error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, async () => {
  await sequelize.sync();
  console.log(`Server is running on port ${PORT}`);
});
