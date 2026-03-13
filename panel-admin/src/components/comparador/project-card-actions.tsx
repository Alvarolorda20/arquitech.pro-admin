'use client';

import {useId, useState} from 'react';
import {useFormStatus} from 'react-dom';
import Link from 'next/link';
import {Button, Modal} from '@gravity-ui/uikit';
import {AlertTriangle} from 'lucide-react';

import styles from './project-card-actions.module.css';

interface ProjectCardActionsProps {
  projectId: string;
  tenantId: string;
  projectName: string;
  openLabel: string;
  deleteLabel: string;
  readOnly?: boolean;
  modalTitle: string;
  modalDescription: string;
  modalCancel: string;
  modalConfirm: string;
  deleteAction: (formData: FormData) => void | Promise<void>;
}

interface DeleteSubmitButtonProps {
  confirmLabel: string;
  canDelete: boolean;
}

function DeleteSubmitButton({confirmLabel, canDelete}: DeleteSubmitButtonProps) {
  const {pending} = useFormStatus();
  const disabled = pending || !canDelete;

  return (
    <Button type="submit" view="action" size="m" className={styles.deleteButton} disabled={disabled}>
      {pending ? 'Eliminando...' : confirmLabel}
    </Button>
  );
}

export function ProjectCardActions({
  projectId,
  tenantId,
  projectName,
  openLabel,
  deleteLabel,
  readOnly = false,
  modalTitle,
  modalDescription,
  modalCancel,
  modalConfirm,
  deleteAction,
}: ProjectCardActionsProps) {
  const [confirmOpen, setConfirmOpen] = useState(false);
  const [confirmationText, setConfirmationText] = useState('');
  const modalTitleId = useId();
  const modalDescriptionId = useId();
  const modalInputId = useId();
  const resolvedTitle = 'Eliminar proyecto';
  const projectNameTrimmed = projectName.trim();
  const isNameConfirmed =
    confirmationText.trim().toLowerCase() === projectNameTrimmed.toLowerCase();
  const titleAttribute =
    modalTitle?.trim() && modalTitle.trim() !== resolvedTitle ? modalTitle.trim() : undefined;

  return (
    <>
      <div className={styles.actions}>
        <Link href={`/products/comparacion-presupuestos?project=${projectId}`}>
          <Button view="outlined-action" size="m">
            {openLabel}
          </Button>
        </Link>
        {!readOnly ? (
          <Button view="flat-danger" size="m" onClick={() => setConfirmOpen(true)}>
            {deleteLabel}
          </Button>
        ) : null}
      </div>

      <Modal
        open={confirmOpen}
        onOpenChange={(open) => {
          setConfirmOpen(open);
          if (!open) {
            setConfirmationText('');
          }
        }}
      >
        <div
          className={styles.modal}
          role="dialog"
          aria-modal="true"
          aria-labelledby={modalTitleId}
          aria-describedby={modalDescriptionId}
        >
          <header className={styles.modalHeader}>
            <h3 id={modalTitleId} className={styles.modalTitle} title={titleAttribute}>
              <AlertTriangle className={styles.modalTitleIcon} aria-hidden="true" />
              <span>{resolvedTitle}</span>
            </h3>
          </header>

          <div className={styles.modalBody}>
            <p id={modalDescriptionId} className={styles.modalText}>
              Se eliminara <strong>{projectNameTrimmed}</strong> con todo su historial. Esta accion
              no se puede deshacer.
            </p>
            <label htmlFor={modalInputId} className={styles.modalInputLabel}>
              Escribe el nombre del proyecto para confirmar
            </label>
            <input
              id={modalInputId}
              name="confirmProjectName"
              type="text"
              value={confirmationText}
              onChange={(event) => setConfirmationText(event.target.value)}
              className={styles.modalInput}
              placeholder={projectNameTrimmed}
              autoComplete="off"
            />
          </div>

          <footer className={styles.modalFooter}>
            <form action={deleteAction} className={styles.modalActions}>
              <input type="hidden" name="tenantId" value={tenantId} />
              <input type="hidden" name="projectId" value={projectId} />
              <Button
                type="button"
                view="outlined"
                size="m"
                className={styles.cancelButton}
                onClick={() => {
                  setConfirmOpen(false);
                  setConfirmationText('');
                }}
              >
                {modalCancel}
              </Button>
              <DeleteSubmitButton confirmLabel={modalConfirm} canDelete={isNameConfirmed} />
            </form>
          </footer>
        </div>
      </Modal>
    </>
  );
}
